import asyncio
import logging
import os
from asyncio import Lock
from datetime import datetime
from typing import List, Tuple

import aiohttp
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Response, HTTPException
from remnawave_api import RemnawaveSDK

# Загрузка переменных окружения
load_dotenv()

# ================== НАСТРОЙКИ ==================
SUBSCRIPTION_SOURCE_URL = os.getenv("SUBSCRIPTION_SOURCE_URL")
CHECK_TIMEOUT = int(os.getenv("CHECK_TIMEOUT", "10"))  # таймаут проверки хоста в секундах
MAX_CONCURRENT_CHECKS = int(os.getenv("MAX_CONCURRENT_CHECKS", "50"))  # макс. одновременных проверок
UPDATE_INTERVAL_MINUTES = int(os.getenv("UPDATE_INTERVAL_MINUTES", "1"))
APP_PORT = int(os.getenv("APP_PORT", "3100"))
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")

# Настройки Remnawave
REMNAWAVE_BASE_URL = os.getenv("REMNAWAVE_BASE_URL")  # URL панели Remnawave
REMNAWAVE_TOKEN = os.getenv("REMNAWAVE_TOKEN")  # API токен из панели
# Проверка наличия обязательных переменных
if not REMNAWAVE_BASE_URL or not REMNAWAVE_TOKEN:
    raise ValueError("REMNAWAVE_BASE_URL и REMNAWAVE_TOKEN должны быть установлены в .env файле")

# Заголовки ответа
RESPONSE_HEADERS = {
    "Content-Type": "text/plain; charset=utf-8",
    "Profile-Update-Interval": str(UPDATE_INTERVAL_MINUTES),
    "Cache-Control": "no-cache"
}
# =================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Subscription API", description="")
# Глобальные переменные с Read-Write Lock
working_configs_cache: List[str] = []
last_update_time = None
background_update_task = None

# Блокировки для потокобезопасного доступа к кэшу
read_lock = Lock()  # Блокировка для читателей
write_lock = Lock()  # Блокировка для писателей
current_readers = 0
readers_lock = Lock()


class ReadWriteLock:
    """Простая реализация Read-Write Lock для asyncio"""

    def __init__(self):
        self._read_ready = asyncio.Condition()
        self._write_ready = asyncio.Condition()
        self._readers = 0
        self._writers = 0
        self._waiting_writers = 0

    async def acquire_read(self):
        """Блокировка для чтения"""
        async with self._read_ready:
            while self._writers > 0 or self._waiting_writers > 0:
                await self._read_ready.wait()
            self._readers += 1

    async def release_read(self):
        """Освобождение блокировки чтения"""
        async with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    async def acquire_write(self):
        """Блокировка для записи"""
        async with self._write_ready:
            self._waiting_writers += 1
            while self._readers > 0 or self._writers > 0:
                await self._write_ready.wait()
            self._waiting_writers -= 1
            self._writers = 1

    async def release_write(self):
        """Освобождение блокировки записи"""
        async with self._write_ready:
            self._writers = 0
            self._write_ready.notify_all()

    def reader_lock(self):
        """Контекстный менеджер для чтения"""
        return _ReaderLock(self)

    def writer_lock(self):
        """Контекстный менеджер для записи"""
        return _WriterLock(self)


class _ReaderLock:
    def __init__(self, rwlock: ReadWriteLock):
        self.rwlock = rwlock

    async def __aenter__(self):
        await self.rwlock.acquire_read()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.rwlock.release_read()


class _WriterLock:
    def __init__(self, rwlock: ReadWriteLock):
        self.rwlock = rwlock

    async def __aenter__(self):
        await self.rwlock.acquire_write()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.rwlock.release_write()


# Инициализируем RW Lock
rw_lock = ReadWriteLock()


# Кэш с версиями для атомарного обновления
class AtomicCache:
    """Атомарный кэш с версионированием для мгновенного переключения"""

    def __init__(self):
        self._cache_version = 0
        self._caches = {}  # version -> cache_data
        self._current_version = 0

    async def update(self, new_data: List[str]) -> int:
        """Атомарно обновляет кэш, возвращает новую версию"""
        async with rw_lock.writer_lock():
            new_version = self._cache_version + 1
            self._caches[new_version] = new_data
            self._current_version = new_version
            self._cache_version = new_version

            # Очищаем старые версии (оставляем только текущую и предыдущую)
            old_versions = [v for v in self._caches.keys() if v < new_version - 1]
            for v in old_versions:
                del self._caches[v]

            return new_version

    async def get(self) -> Tuple[List[str], int]:
        """Получает текущие данные кэша и версию"""
        async with rw_lock.reader_lock():
            version = self._current_version
            data = self._caches.get(version, [])
            # Возвращаем копию данных, чтобы избежать изменений
            return data.copy(), version

    async def get_version(self) -> int:
        """Получает текущую версию кэша"""
        async with rw_lock.reader_lock():
            return self._current_version


# Используем атомарный кэш вместо простого списка
atomic_cache = AtomicCache()


async def verify_user(short_uuid: str):
    """Проверяет существование пользователя в Remnawave."""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{REMNAWAVE_BASE_URL}/api/users/by-short-uuid/{short_uuid}"
            headers = {"Authorization": f"Bearer {REMNAWAVE_TOKEN}"}

            async with session.get(url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    response = data.get("response")
                    if response.get('status') == 'ACTIVE':
                        logger.info(f"✅ Пользователь {response.get('username')} авторизован")
                        return response
                    else:
                        logger.warning(f"❌ Пользователь не активен")
                        return None
                else:
                    logger.warning(f"❌ Пользователь не найден")
                    return None
    except Exception as e:
        logger.error(f"Ошибка при проверке пользователя: {e}")
        return None


async def fetch_subscription_file(session: aiohttp.ClientSession, url: str) -> str:
    """Скачивает файл подписки по URL."""
    try:
        async with session.get(url, timeout=CHECK_TIMEOUT) as resp:
            if resp.status == 200:
                content = await resp.text()
                logger.info(f"Файл подписки загружен, размер: {len(content)} байт")
                return content
            else:
                logger.error(f"Ошибка загрузки {url}: HTTP {resp.status}")
                return ""
    except Exception as e:
        logger.error(f"Ошибка при загрузке {url}: {e}")
        return ""


async def extract_host_port(config_line: str) -> Tuple[str, int]:
    """Извлекает хост и порт из URI конфига."""
    import re
    import base64
    import json

    if config_line.startswith("vmess://"):
        try:
            encoded = config_line[8:]
            decoded = base64.b64decode(encoded).decode('utf-8')
            config_json = json.loads(decoded)
            host = config_json.get("add", "")
            port = int(config_json.get("port", 0))
            if host and port:
                return host, port
        except:
            pass

    patterns = [
        (r'(?:vless|trojan|ss|ssr)://[^@]+@([^:]+):(\d+)', lambda m: (m.group(1), int(m.group(2)))),
        (r'(?:vless|trojan|ss|ssr)://([^:]+):(\d+)', lambda m: (m.group(1), int(m.group(2)))),
    ]

    for pattern, extractor in patterns:
        match = re.search(pattern, config_line)
        if match:
            try:
                return extractor(match)
            except:
                continue

    return None, None


async def check_host_availability(config_line: str) -> Tuple[str, bool]:
    """Проверяет доступность V2Ray конфига."""
    if not any(config_line.startswith(prefix) for prefix in ["vmess://", "vless://", "trojan://", "ss://", "ssr://"]):
        return config_line, False

    try:
        host, port = await extract_host_port(config_line)
        if not host or not port:
            return config_line, False

        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=CHECK_TIMEOUT
            )
            writer.close()
            await writer.wait_closed()
            return config_line, True
        except:
            return config_line, False
    except:
        return config_line, False


async def update_working_configs(force: bool = False):
    """
    Обновляет список рабочих конфигов в фоне.
    Во время обновления старый кэш продолжает использоваться для ответов.
    """
    # Проверяем, нужно ли обновление
    current_version = await atomic_cache.get_version()

    if not force and last_update_time:
        age_minutes = (datetime.now() - last_update_time).total_seconds() / 60
        if age_minutes < UPDATE_INTERVAL_MINUTES:
            logger.info(f"Кэш свежий (версия {current_version}, возраст: {age_minutes:.1f} мин), пропускаю обновление")
            return

    logger.info(f"🔄 Начинаю обновление конфигов (текущая версия кэша: {current_version})")
    start_time = datetime.now()

    try:
        async with aiohttp.ClientSession() as session:
            # Скачиваем файл
            raw_content = await fetch_subscription_file(session, SUBSCRIPTION_SOURCE_URL)
            if not raw_content:
                logger.error("Не удалось получить исходный файл подписки")
                return

            configs = [line.strip() for line in raw_content.splitlines() if line.strip()]
            logger.info(f"Загружено конфигов для проверки: {len(configs)}")

            # Проверяем доступность с прогрессом
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)

            async def limited_check(cfg):
                async with semaphore:
                    return await check_host_availability(cfg)
            # для теста
            tasks = [limited_check(cfg) for cfg in configs[1:50]]
            results = []

            # Отслеживаем прогресс без блокировки основного цикла
            for i, coro in enumerate(asyncio.as_completed(tasks)):
                result = await coro
                results.append(result)
                if (i + 1) % 100 == 0:
                    logger.info(f"Прогресс проверки: {i + 1}/{len(configs)}")

            # Фильтруем рабочие конфиги
            new_working_configs = [cfg for cfg, is_ok in results if is_ok]

            # Атомарно обновляем кэш
            new_version = await atomic_cache.update(new_working_configs)

            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"✅ Обновление завершено за {elapsed:.1f} сек. "
                        f"Рабочих конфигов: {len(new_working_configs)} из {len(configs)}. "
                        f"Новая версия кэша: {new_version}")

            last_update_time = datetime.now()

    except Exception as e:
        logger.error(f"Ошибка при обновлении кэша: {e}", exc_info=True)


async def background_updater():
    """Фоновая задача для периодического обновления подписок."""
    while True:
        try:
            await asyncio.sleep(UPDATE_INTERVAL_MINUTES * 60)
            logger.info(f"⏰ Плановое обновление подписок (интервал: {UPDATE_INTERVAL_MINUTES} мин)")
            await update_working_configs(force=False)
        except Exception as e:
            logger.error(f"Ошибка в фоновом обновлении: {e}")
            await asyncio.sleep(60)  # При ошибке ждём минуту и продолжаем


@app.on_event("startup")
async def startup_event():
    """При старте сервера обновляем кэш и запускаем фоновое обновление."""
    global background_update_task

    logger.info(f"🚀 Запуск сервера. Интервал обновления: {UPDATE_INTERVAL_MINUTES} минут")
    logger.info(f"🔐 Remnawave API: {REMNAWAVE_BASE_URL}")

    # Первоначальное обновление
    await update_working_configs(force=True)

    # Запускаем фоновую задачу
    background_update_task = asyncio.create_task(background_updater())
    logger.info("✅ Фоновое обновление запущено")


@app.on_event("shutdown")
async def shutdown_event():
    """Останавливаем фоновую задачу при выключении сервера."""
    if background_update_task:
        background_update_task.cancel()
        try:
            await background_update_task
        except asyncio.CancelledError:
            logger.info("Фоновое обновление остановлено")


@app.get("/subscription/{short_uuid}")
async def get_subscription_by_short_uuid(
        short_uuid: str,
        response: Response,
        format_type: str = "plain"
):
    """
    Эндпоинт подписки с авторизацией через short_uuid.
    Всегда доступен, даже во время обновления кэша.
    """
    # Проверяем пользователя (можно закэшировать)
    user = await verify_user(short_uuid)
    if not user:
        raise HTTPException(status_code=403, detail="Unauthorized: Invalid or inactive user")

    # Получаем текущие конфиги из атомарного кэша (мгновенно, без блокировки)
    configs, cache_version = await atomic_cache.get()

    if not configs:
        # Если кэш пустой, пробуем обновить в фоне, но возвращаем то что есть
        logger.warning("Кэш пуст, возможно первое обновление ещё не завершено")
        asyncio.create_task(update_working_configs(force=False))
        return Response(content="", media_type="text/plain")

    # Формируем ответ
    body = "\n".join(configs)

    if format_type.lower() == "base64":
        import base64
        body = base64.b64encode(body.encode()).decode()

    # Добавляем заголовки с информацией о кэше
    for name, value in RESPONSE_HEADERS.items():
        response.headers[name] = value

    response.headers["X-Cache-Version"] = str(cache_version)
    response.headers["X-Cache-Age-Minutes"] = str(
        int((datetime.now() - last_update_time).total_seconds() / 60) if last_update_time else 0
    )
    response.headers["X-User-Name"] = user.get("username")

    return Response(content=body, media_type="text/plain")


@app.get("/subscription/by-uuid/{user_uuid}")
async def get_subscription_by_uuid(
        user_uuid: str,
        response: Response,
        format_type: str = "plain"
):
    """Альтернативный эндпоинт - авторизация по полному UUID."""
    user = await verify_user_by_uuid(user_uuid)
    if not user:
        raise HTTPException(status_code=403, detail="Unauthorized: Invalid or inactive user")

    configs, cache_version = await atomic_cache.get()

    body = "\n".join(configs) if configs else ""

    if format_type.lower() == "base64" and body:
        import base64
        body = base64.b64encode(body.encode()).decode()

    for name, value in RESPONSE_HEADERS.items():
        response.headers[name] = value

    response.headers["X-Cache-Version"] = str(cache_version)

    return Response(content=body, media_type="text/plain")


@app.get("/status")
async def get_status():
    """Информация о состоянии сервиса."""
    configs, cache_version = await atomic_cache.get()

    age_minutes = 0
    if last_update_time:
        age_minutes = (datetime.now() - last_update_time).total_seconds() / 60

    return {
        "status": "ok",
        "cached_configs": len(configs),
        "cache_version": cache_version,
        "last_update": last_update_time.isoformat() if last_update_time else None,
        "last_update_minutes_ago": round(age_minutes, 1),
        "update_interval_minutes": UPDATE_INTERVAL_MINUTES,
        "remnawave_connected": bool(REMNAWAVE_BASE_URL and REMNAWAVE_TOKEN)
    }


@app.get("/health")
async def health_check():
    """Проверка работоспособности."""
    configs, _ = await atomic_cache.get()
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "configs_available": len(configs) > 0,
        "accepting_connections": True
    }


@app.get("/metrics")
async def get_metrics():
    """Эндпоинт для мониторинга (Prometheus format)."""
    configs, cache_version = await atomic_cache.get()

    return {
        "cache_version": cache_version,
        "total_configs": len(configs),
        "last_update_seconds_ago": int((datetime.now() - last_update_time).total_seconds()) if last_update_time else -1,
        "update_interval_seconds": UPDATE_INTERVAL_MINUTES * 60
    }


async def verify_user_by_uuid(uuid: str):
    """Проверяет пользователя по полному UUID."""
    try:

        sdk = RemnawaveSDK(base_url=REMNAWAVE_BASE_URL, token=REMNAWAVE_TOKEN)
        user = await sdk.users.get_user_by_uuid(uuid)

        if user and user.status == "active":
            logger.info(f"✅ Пользователь {user.username} авторизован по UUID")
            return user
        else:
            logger.warning(f"❌ Пользователь с UUID {uuid} не активен")
            return None

    except Exception as e:
        logger.error(f"Ошибка при проверке пользователя по UUID: {e}")
        return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="VPN Subscription API with Remnawave Auth")
    parser.add_argument("--host", default=APP_HOST, help="Хост для прослушивания")
    parser.add_argument("--port", type=int, default=APP_PORT, help="Порт для прослушивания")
    parser.add_argument("--reload", action="store_true", help="Режим перезагрузки при разработке")
    args = parser.parse_args()

    uvicorn.run(app, host=args.host, port=args.port, reload=args.reload)