using StackExchange.Redis;
using Newtonsoft.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Runtime.InteropServices;
using System.Net;
using System.Threading;

namespace Cache
{
    public class RedisCache<T>
    {
        #region Connection Initialize
        private IConnectionMultiplexer Connection;
        IHttpContextAccessor Context;
        ConfigurationOptions configurationOptions = new ConfigurationOptions();
        private readonly SemaphoreSlim semaphoreSlim = new(1, 1);

        IConfiguration Configuration;
        IConfigurationSection RedisConnection;
        private ILogger<RedisCache<T>> Logger;
        private string EntityKey;
        private string TenantKey = "1";
        private string LoginId = "1";
        private string CompanyId;
        private string MaxRoleGroup;

        public RedisCache(IConnectionMultiplexer Connection, IConfiguration Configuration, ILogger<RedisCache<T>> Logger, IHttpContextAccessor Context)
        {
            this.Connection = Connection;
            this.Logger = Logger;
            this.Configuration = Configuration;
            RedisConnection = Configuration.GetSection("RedisCache");
            this.Context = Context;
            //var res=  ClusterFailOverAsync().Result;
        }
        private async ValueTask<IConnectionMultiplexer> ConnectAsync()
        {
            Configuration.GetSection("RedisCache:EndPoints").GetChildren().Select(s => s.Value).ToList().ForEach(f =>
             configurationOptions.EndPoints.Add(f));
            configurationOptions.AbortOnConnectFail = false;
            configurationOptions.User = Configuration.GetValue<string>("RedisCache:User", null);
            configurationOptions.Password = RedisConnection["Password"];
            configurationOptions.AllowAdmin = true;// Configuration.GetValue<bool>("RedisCache:AllowAdmin", false);
            var commandMap = CommandMap.Create(new HashSet<string>
            { // EXCLUDE a few commands
                "INFO", "CONFIG", "CLUSTER",
              "PING", "ECHO", "CLIENT"
            }, available: false);
            //configurationOptions.CommandMap = commandMap;
            configurationOptions.DefaultDatabase = Configuration.GetValue<int?>("RedisCache:Database", null);
            var con = await ConnectionMultiplexer.ConnectAsync(configurationOptions);
            return con;
        }
        public async ValueTask ReconnectAsync()
        {
            await semaphoreSlim.WaitAsync();
            try
            {
                if (Connection != null)
                {
                   await Connection.CloseAsync();
                   await Connection.DisposeAsync();
                }
                Connection = await ConnectAsync();
            }
            catch (System.Exception)
            {
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }
        public IConnectionMultiplexer GetRedisConnection()
        {
            return this.Connection;
        }
        //Get Db
        private IDatabase GetRedisDb()
        {
            return Connection.GetDatabase();
        }
        #endregion

        #region[External function]
        //Serialize object
        private string Serialize(object obj)
        {
            return JsonConvert.SerializeObject(obj);
        }
        //Deserialize object
        private T Deserialize(string serialized)
        {
            return JsonConvert.DeserializeObject<T>(serialized);
        }
        #endregion
        #region[GenericFunction]
        /// <summary>
        /// Insert Data 
        /// </summary>
        /// <param name="key">Primary key</param>
        /// <param name="value">object value</param>
        public bool Add(object key, T value, bool SetExpiry = false, int ExpiryDuration = 3600)
        {
            var res = GetRedisDb().HashSet(RedisKey, Serialize(key), Serialize(value));
            if (SetExpiry)
                try { var ExpiryResult = GetRedisDb().Execute("hexpire", RedisKey, ExpiryDuration, "FIELDS", "1", Serialize(key)); }
                catch (System.Exception e) { Logger.LogError("Redis auto expire error! Try using redis 7.4.0 or above " + e.ToString()); }
            return res;
        }
        public async Task<bool> AddAsync(object key, T value, bool SetExpiry = false, int ExpiryDuration = 3600)
        {
            bool addTask = await GetRedisDb().HashSetAsync(RedisKey, Serialize(key), Serialize(value));
            if (SetExpiry)
                try { var ExpiryResult = await GetRedisDb().ExecuteAsync("hexpire", RedisKey, ExpiryDuration, "FIELDS", "1", Serialize(key)); }
                catch (System.Exception e) { Logger.LogError("Redis auto key expire error! " + e.ToString()); }
            return addTask;
        }
        /// <summary>
        /// Add or Update 
        /// </summary>
        /// <param name="item">keyvaluepair key,value</param>
        public bool Add(KeyValuePair<object, T> item, bool SetExpiry = false, int ExpiryDuration = 3600)
        {
            var res = Add(item.Key, item.Value, SetExpiry, ExpiryDuration);
            return res;
        }
        public async Task<bool> AddAsync(KeyValuePair<object, T> item, bool SetExpiry = false, int ExpiryDuration = 3600)
        {
            var res = await AddAsync(item.Key, item.Value, SetExpiry, ExpiryDuration);
            return res;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="items"></param>
        /// <param name="SetExpiry">If true, auto expire will be performed for all the adding records in this add operation</param>
        /// <param name="KeyExpiry">KeyExpiry - Auto expire the entire hash data for this tenant </param>
        /// <param name="ExpiryFields">Auto expire the given child records. (It ignores the default operation of auto expire the all adding records of this add operation)</param>
        /// <param name="ExpiryDuration">Duration in seconds</param>
        public void AddMultiple(IEnumerable<KeyValuePair<object, T>> items, bool SetExpiry = false, bool KeyExpiry = false, IEnumerable<string> ExpiryFields = default, int ExpiryDuration = 3600)
        {
            GetRedisDb().HashSet(RedisKey, items.Select(i => new HashEntry(Serialize(i.Key), Serialize(i.Value))).ToArray());
            if (SetExpiry)
            {
                if (KeyExpiry)
                    GetRedisDb().KeyExpire(RedisKey, new TimeSpan(0, 0, ExpiryDuration));
                else
                {
                    ExpiryFields = (ExpiryFields != null && ExpiryFields.Count() > 0) ? ExpiryFields.Select(s => Serialize(s)) : items.Select(s => Serialize(s.Key));
                    List<object> parm = new() { RedisKey, ExpiryDuration, "FIELDS", ExpiryFields.Count() };
                    foreach (var item in CollectionsMarshal.AsSpan(ExpiryFields.ToList()))
                        parm.Add(item);
                    try { var ExpiryResult = GetRedisDb().Execute("hexpire", parm); }
                    catch (System.Exception e) { Logger.LogError("Redis auto expire error! Try using redis 7.4.0 or above! " + e.ToString()); }
                }
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="items"></param>
        /// <param name="SetExpiry">If true, auto expire will be performed for all the adding records in this add operation</param>
        /// <param name="KeyExpiry">KeyExpiry - Auto expire the entire hash data for this tenant </param>
        /// <param name="ExpiryFields">Auto expire the given child records. (It ignores the default operation of auto expire the all adding records of this add operation)</param>
        /// <param name="ExpiryDuration">Duration in seconds</param>
        public async Task AddMultipleAsync(IEnumerable<KeyValuePair<object, T>> items, bool SetExpiry = false, bool KeyExpiry = false, IEnumerable<string> ExpiryFields = default, int ExpiryDuration = 3600)
        {
            await GetRedisDb().HashSetAsync(RedisKey, items.Select(i => new HashEntry(Serialize(i.Key), Serialize(i.Value))).ToArray());
            if (SetExpiry)
            {
                if (KeyExpiry)
                {
                    var keyExpiryResult = await GetRedisDb().KeyExpireAsync(RedisKey, new TimeSpan(0, 0, ExpiryDuration));
                }
                else
                {
                    ExpiryFields = (ExpiryFields != null && ExpiryFields.Count() > 0) ? ExpiryFields.Select(s => Serialize(s)) : items.Select(s => Serialize(s.Key));
                    List<object> parm = new() { RedisKey, ExpiryDuration, "FIELDS", ExpiryFields.Count() };
                    Parallel.ForEach(ExpiryFields, item =>
                    {
                        parm.Add(item);
                    });
                    try { var ExpiryResult = await GetRedisDb().ExecuteAsync("hexpire", parm); }
                    catch (System.Exception e) { Logger.LogError("Redis auto key expire error! " + e.ToString()); }
                }
            }
        }
        /// <summary>
        /// Get Contains 
        /// </summary>
        /// <param name="key">pass key</param>
        /// <returns>True or false</returns>
        public bool ContainsKey(object key)
        {
            var res = GetRedisDb().HashExists(RedisKey, Serialize(key));

            return res;
        }
        public async Task<bool> ContainsKeyAsync(object key)
        {
            bool res = await GetRedisDb().HashExistsAsync(RedisKey, Serialize(key));

            return res;
        }
        /// <summary>
        /// Remove 
        /// </summary>
        /// <param name="key">pass param Key</param>
        /// <returns>true or false</returns>
        public bool Remove(object key)
        {
            var res = GetRedisDb().HashDelete(RedisKey, Serialize(key));

            return res;
        }
        public async Task<bool> RemoveAsync(object key)
        {
            var res = await GetRedisDb().HashDeleteAsync(RedisKey, Serialize(key));

            return res;
        }
        /// <summary>
        /// remove
        /// </summary>
        /// <param name="item">pass keyvaluepair</param>
        /// <returns></returns>
        public bool Remove(KeyValuePair<object, T> item)
        {
            return Remove(item.Key);
        }
        public async Task<bool> RemoveAsync(KeyValuePair<object, T> item)
        {
            return await RemoveAsync(item.Key);
        }
        /// <summary>
        /// Delete the All records based key
        /// </summary>
        public bool Clear()
        {
            var res = GetRedisDb().KeyDelete(RedisKey);
            return res;
        }
        public async Task<bool> ClearAsync()
        {
            var res = await GetRedisDb().KeyDeleteAsync(RedisKey);

            return res;
        }
        /// <summary>
        /// Get Single Entity based on key
        /// </summary>
        /// <param name="key">primary key</param>
        /// <returns>return entity</returns>
        public T Get(object key)
        {
            var redisValue = GetRedisDb().HashGet(RedisKey, Serialize(key));
            if (redisValue.IsNull)
                return default(T);
            return Deserialize(redisValue.ToString());
        }
        public async Task<T> GetAsync(object key)
        {
            var redisValue = await GetRedisDb().HashGetAsync(RedisKey, Serialize(key));
            if (redisValue.IsNull)
                return default(T);
            return Deserialize(redisValue.ToString());
        }
        /// <summary>
        /// Get All Data based on key
        /// </summary>
        /// <returns>retrun list of data</returns>
        public List<T> GetAll()
        {
            var db = GetRedisDb();
            var list = db.HashGetAll(RedisKey).Select(s => Deserialize(s.Value.ToString()));
            return new List<T>(list);
        }
        public async Task<IEnumerable<T>> GetAllAsync()
        {
            var db = GetRedisDb();
            var res = await db.HashGetAllAsync(RedisKey);
            return res.Select(s => Deserialize(s.Value.ToString()));
        }

        /// <summary>
        /// Get Count based on key
        /// </summary>
        public long Count()
        {
            var res = GetRedisDb().HashLength(RedisKey);
            return res;
        }

        public async Task<long> CountAsync()
        {
            var res = await GetRedisDb().HashLengthAsync(RedisKey);
            return res;
        }
        public bool StringSet(string Key, string Value, bool IsAutoExpire = false)
        {
            var ExpiryDuration = Configuration["ApplicationTimeOut"] ?? "3600";
            return GetRedisDb().StringSet(Key, Value, (IsAutoExpire ? new TimeSpan(0, 0, Convert.ToInt32(ExpiryDuration)) : (TimeSpan?)null));
        }
        public string StringGet(string Key)
        {
            return GetRedisDb().StringGet(Key);
        }
        public async Task<string> StringGetAsync(string Key)
        {
            return await GetRedisDb().StringGetAsync(Key);
        }
        public bool StringDelete(string Key)
        {
            return GetRedisDb().KeyDelete(Key);
        }
        public async Task<bool> KeyDeleteAsync(string Key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await GetRedisDb().KeyDeleteAsync(Key);
        }
        public void SetList(string Key, IEnumerable<dynamic> Values)
        {
            //GetRedisDb().ListLeftPush(Key, Values.Select(s=> { return Serialize(s); }));

            Values.ToList().ForEach(f => { GetRedisDb().ListLeftPush(Key, Serialize(f)); });
        }
        public IEnumerable<T> GetList(string Key)
        {
            var length = GetRedisDb().ListLength(Key);
            var redisValue = GetRedisDb().ListRange(Key, 0, length - 1);
            //if (typeof(T) == typeof(string))
            //{
            var res = redisValue.Select(s => JsonConvert.DeserializeObject<T>(Serialize(s)));
            return res;
            //}
            //else
            //    return redisValue.Select(s => JsonConvert.DeserializeObject<dynamic>(s));
        }
        public async Task<bool> SetRemoveAsync(RedisKey Key, RedisValue Value, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var IsSetRemoved = await GetRedisDb().SetRemoveAsync(Key, Value);
            return IsSetRemoved;
        }
        public async Task<Dictionary<string, Task<RedisType>>> GetKeysBypatternAsync(string Pattern, CancellationToken Token = default)
        {
            Token.ThrowIfCancellationRequested();
            IDatabase database = GetRedisDb();
            //var redisResult = (RedisResult[])GetRedisDb().ScriptEvaluate("return redis.call('keys', 'IdServ:*')");
            var redisResult = (RedisResult[])await GetRedisDb().ScriptEvaluateAsync($"return redis.call('keys', '{Pattern}')");
            //var keys = System.Text.Json.JsonSerializer.Deserialize<List<dynamic>>(System.Text.Json.JsonSerializer.Serialize(redisResult));
            var keys = JsonConvert.DeserializeObject<IEnumerable<string>>(JsonConvert.SerializeObject(redisResult));
            var keysWithTypes = keys.ToDictionary(k => k, async k => await database.KeyTypeAsync(k));
            return keysWithTypes;
        }
        public StackExchange.Redis.IServer GetConnectedServer(CancellationToken Token = default)
        {
            Token.ThrowIfCancellationRequested();
            StackExchange.Redis.IServer server = Connection.GetServers()?.FirstOrDefault(f => f.IsConnected);// ("localhost", 6379); // Default Redis port
            return server;
        }
        #endregion
        #region Cluster
        public async ValueTask<bool> ClusterFailOverAsync(CancellationToken cancellationToken=default)
        {
            var nodeInfo = GetNodes(Connection);
            if (nodeInfo.IsCluster == false) return false;
            //Set FailOver if no master available
            if (nodeInfo.Master?.Node == null)
            {
                if (nodeInfo.Replica?.Node == null) return false;
                var isFailOvered = await nodeInfo.Replica.Server.ExecuteAsync("CLUSTER", "FAILOVER", "FORCE");
                await ReconnectAsync();
                nodeInfo = GetNodes(Connection);
            }
            //set replica if it has no replicate instance
            if (nodeInfo.Master?.Node is not null && nodeInfo.Slaves.Any())
            {
                var slaves = nodeInfo.Slaves.ToList();
                var ParallelOptions = new ParallelOptions { MaxDegreeOfParallelism = 1,CancellationToken= cancellationToken };
                await Parallel.ForEachAsync(slaves, ParallelOptions, async (slave, ParallelOptions) =>
                {
                    await slave.Server.ReplicaOfAsync(nodeInfo.Master.Node.EndPoint);
                    //var res = await slaveServer.ExecuteAsync("CLUSTER", "REPLICATE", nodeInfo.Master.NodeId);
                });
            }
            ////var a3 = StringSet("a8", "c8");
            //var ckeys = (IEnumerable<string>)GetList("Protection-Keys-Grid");
            //SetList("test", new string[] { "test1", "test2", "test3" });
            //var ckeys1 = (IEnumerable<string>)GetList("test");
            return true;
        }
        private (bool IsCluster, GarnetNode Master, GarnetNode Replica, IEnumerable<GarnetNode> Slaves) GetNodes(IConnectionMultiplexer connection)
        {
            GarnetNode Master = new GarnetNode(), Replica = new GarnetNode(); var Slaves = Enumerable.Empty<GarnetNode>();
            var Servers = connection.GetServers()?.Where(w => w.IsConnected);
            var Server = Servers?.FirstOrDefault();

            if (Server == null || Server.ServerType != ServerType.Cluster)
                return (IsCluster: false, Master, Replica, Slaves);

            var nodes = Server.ClusterConfiguration.Nodes;
            
            Master = (from n in nodes  //(w => w.Slots.Count > 0 && w.IsConnected == true)
                      join s in Servers on n.EndPoint equals s.EndPoint
                      where n.Slots.Count > 0 && (n.IsConnected == true && n.IsFail == false && n.IsPossiblyFail==false)
                      select new GarnetNode { Node = n, Server = s }).FirstOrDefault();

            Replica = (from n in nodes  //(f => f.Slots.Count < 1 && f.IsConnected == true && f.IsReplica);
                       join s in Servers on n.EndPoint equals s.EndPoint
                       where n.Slots.Count < 1 && n.IsConnected == true && n.IsReplica
                       select new GarnetNode { Node = n, Server = s }).FirstOrDefault();

            Slaves = from n in nodes //nodes.Where(w => w.Slots.Count < 1 && w.IsConnected == true && !w.IsReplica);
                     join s in Servers on n.EndPoint equals s.EndPoint
                     where n.Slots.Count < 1 && n.IsConnected == true && !n.IsReplica
                     select new GarnetNode { Node = n, Server = s };
            return (IsCluster: true, Master, Replica, Slaves);
        }
        public class GarnetNode
        {
            public ClusterNode Node { get; set; }
            public StackExchange.Redis.IServer Server { get; set; }
        }
        #endregion Cluster

        #region Properties
        public int AdminAssignedCompanyId { get; set; }

        private string _RedisKey;
        private string RedisKey
        {
            get
            {
                // var a = Helper.Utilities.HttpContext.Authentication.GetTokenAsync("access_token").Result;
                //Context = Helper.Utilities.HttpContext;
                IEnumerable<string> EntityKeysWithoutTenant = new string[] { "TimeZoneCore" };
                EntityKey = typeof(T).Name;
                if (Context.HttpContext.User.Claims != null && Context.HttpContext.User.Claims.Count() > 0)
                {
                    LoginId = Context.HttpContext.User.Claims.FirstOrDefault(f => f.Type == "sub").Value;
                    CompanyId = Context.HttpContext.User.Claims.FirstOrDefault(f => f.Type == "CompanyId").Value;

                    MaxRoleGroup = Context.HttpContext.User.Claims.FirstOrDefault(f => f.Type == "MaxRoleGroup")?.Value;
                }

                _RedisKey = EntityKey;

                //Tenant key setting
                if (CompanyId == "1" && MaxRoleGroup == "1" && AdminAssignedCompanyId > 0)
                    TenantKey = Convert.ToString(AdminAssignedCompanyId);
                else
                    TenantKey = CompanyId;
                //Set tenantKey with Redis Key
                if (EntityKeysWithoutTenant.Contains(EntityKey))
                    _RedisKey = EntityKey;
                else
                    _RedisKey = EntityKey + ":" + TenantKey;
                //LoingId(Login User based) key setting                  
                if (EntityKey == "RoleAccess" || EntityKey == "CPRoleAccess")
                    _RedisKey = _RedisKey + ":" + LoginId;

                return _RedisKey;
            }
            set
            {
                _RedisKey = value;
            }
        }

        #endregion

    }
}