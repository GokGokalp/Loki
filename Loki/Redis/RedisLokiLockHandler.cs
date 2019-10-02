using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Loki.Redis
{
    public class RedisLokiLockHandler : LokiLockHandler
    {
        private static IRedisStore _redisStore;
        private static RedisValue _token;
        private readonly ILogger<RedisLokiLockHandler> _logger;

        public RedisLokiLockHandler(EndPoint[] redisEndPoints, ILogger<RedisLokiLockHandler> logger)
        {
            _redisStore = RedisStore.Instance.Initialize(redisEndPoints);
            _token = Environment.MachineName;
            _logger = logger;
        }

        public override bool Lock(string serviceKey, int expiryFromSeconds)
        {
            bool isLocked = false;

            try
            {
                string isAnyLocked = _redisStore.Get(serviceKey);

                if (String.IsNullOrEmpty(isAnyLocked))
                {
                    isLocked = _redisStore.Set(serviceKey, _token, expiryFromSeconds);

                    // When occurs any network problem within Redis, it could be provided consistent locking with secondary handler.
                    if (SecondaryLockHandler != null)
                    {
                        Task.Factory.StartNew(() => SecondaryLockHandler.Lock(serviceKey, expiryFromSeconds));
                    }
                }
            }
            catch (Exception ex)
            {
                if (SecondaryLockHandler != null)
                {
                    isLocked = SecondaryLockHandler.Lock(serviceKey, expiryFromSeconds);
                }
                _logger.LogError($"There was an error while locking for service:{serviceKey}.", ex);
            }

            return isLocked;
        }

        public override void Release(string serviceKey)
        {
            try
            {
                _redisStore.Delete(serviceKey, _token);

                if (SecondaryLockHandler != null)
                {
                    Task.Factory.StartNew(() => SecondaryLockHandler.Release(serviceKey));
                }
            }
            catch (Exception ex)
            {
                SecondaryLockHandler?.Release(serviceKey);
                _logger.LogError($"There was an error while locking for service:{serviceKey}.", ex);
            }
        }
    }
}
