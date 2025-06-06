using Hangfire;
using Hangfire.Redis.StackExchange;
using Hangfire.Storage;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.Configuration;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();

var redisConnectionString = "localhost:6379,DefaultDatabase=0,abortConnect=false,allowAdmin=true";
var Redis = ConnectionMultiplexer.Connect(redisConnectionString);
builder.Services.AddDataProtection()
  .SetApplicationName("App1")
  .PersistKeysToStackExchangeRedis(Redis, "Protection-Keys-Grid");
builder.Services.AddSingleton<IHttpContextAccessor, HttpContextAccessor>();
builder.Services.AddSingleton<IConnectionMultiplexer>(Redis);
builder.Services.AddSingleton(typeof(Cache.RedisCache<>));

var hfRedisOptions = new Hangfire.Redis.StackExchange.RedisStorageOptions
{
    Prefix = "{notification}:",
    DeletedListSize = 20,
    SucceededListSize = 80,
    InvisibilityTimeout = TimeSpan.FromMinutes(30),
    Db = 0
};
builder.Services.AddHangfire(x => x.UseRedisStorage(Redis, options: hfRedisOptions));
builder.Services.AddHangfireServer();
var queueName = Environment.MachineName.ToLower().Replace('-', '_');
var app = builder.Build();
Hangfire.GlobalJobFilters.Filters.Add(new WebSchedulerExpirationTimeAttribute());
app.UseHangfireDashboard("/jobsview", new DashboardOptions()
{
    StatsPollingInterval = (60000), //60000=1 minute
});
RecurringJob.AddOrUpdate("SendSMS", ()=> Console.Write("SendSMS"), "*/30 * * * * *");
RecurringJob.AddOrUpdate("JobTrackingSMS", () => Console.Write("JobTrackingSMS"), "*/30 * * * * *");
RecurringJob.AddOrUpdate("SendEmail", () => Console.Write("SendEmail"), "*/30 * * * * *");
RecurringJob.AddOrUpdate("PushNotification", () => Console.Write("PushNotification"), "*/10 * * * * *");
RecurringJob.AddOrUpdate("PushCustomersToOtherSystem", () => Console.Write("PushCustomersToOtherSystem"), "*/30 * * * * *");
RecurringJob.AddOrUpdate("ClientPortalAutoSchedule", () => Console.Write("ClientPortalAutoSchedule"), "*/30 * * * * *");
RecurringJob.AddOrUpdate("PunchWoCreate", () => Console.Write("PunchWoCreate"), "*/30 * * * * *");
RecurringJob.AddOrUpdate("WorkFlowProcessAction", () => Console.Write("WorkFlowProcessAction"), "*/30 * * * * *");
RecurringJob.AddOrUpdate("AttachmentImport", () => Console.Write("AttachmentImport"), "*/30 * * * * *");
RecurringJob.AddOrUpdate("FormTrackingEmail", () => Console.Write("FormTrackingEmail"), "*/30 * * * * *");
RecurringJob.AddOrUpdate("JobTrackingEmail", () => Console.Write("JobTrackingEmail"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("BulkUpdateAttachment", () => Console.Write("BulkUpdateAttachment"), "*/30 * * * * *");
RecurringJob.AddOrUpdate("ProjectJobCreation", () => Console.Write("ProjectJobCreation"), "*/30 * * * * *");
RecurringJob.AddOrUpdate("ProjectJobDelete", () => Console.Write("ProjectJobDelete"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("GeoTabIntegration", () => Console.Write("GeoTabIntegration"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("GeoTabIntegration", () => Console.Write("GeoTabIntegration"), "*/30 * * * * *");
RecurringJob.AddOrUpdate("UpdateVrRequest", () => Console.Write("UpdateVrRequest"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("IsEnablePasswordExpiryEmail", () => Console.Write("IsEnablePasswordExpiryEmail"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("IsEnableUserInactiveEmail", () => Console.Write("IsEnableUserInactiveEmail"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("AssetIntegration", () => Console.Write("AssetIntegration"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("WebHook", () => Console.Write("WebHook"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("DailyTripSheetEmail", () => Console.Write("DailyTripSheetEmail"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("SendMessage", () => Console.Write("SendMessage"), "*/30 * * * * *");
RecurringJob.AddOrUpdate("RedisCleanup", () => Console.Write("RedisCleanup"), "*/30 * * * * *");
RecurringJob.AddOrUpdate("MerTrackRoute", () => Console.Write("MerTrackRoute"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("MerLocationAssetImport", () => Console.Write("MerLocationAssetImport"), "*/20 * * * * *");

RecurringJob.AddOrUpdate("MerLocationAssetExport", () => Console.Write("MerLocationAssetExport"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("MerEGISImport", () => Console.Write("MerEGISImport"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("MerEGISExport", () => Console.Write("MerEGISExport"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("MerEAMFormsAttachments", () => Console.Write("MerEAMFormsAttachments"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("MerEGISManifestImport", () => Console.Write("MerEGISManifestImport"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("RedisBackupAsync", () => Console.Write("RedisBackupAsync"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("VerizonIntegration", () => Console.Write("VerizonIntegration"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("GeoJsonCreation", () => Console.Write("GeoJsonCreation"), "*/20 * * * * *");
RecurringJob.AddOrUpdate("bulkforminsert", () => Console.Write("bulkforminsert"), "*/20 * * * * *");

// Configure the HTTP request pipeline.
app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();

public class WebSchedulerExpirationTimeAttribute : Hangfire.Common.JobFilterAttribute, Hangfire.States.IApplyStateFilter
{
    public IConfiguration Configuration { get; }
    public WebSchedulerExpirationTimeAttribute()
    {
       
    }
    public void OnStateUnapplied(Hangfire.States.ApplyStateContext context, IWriteOnlyTransaction transaction)
    {
        var JobExpirationTimeout = 3;
        context.JobExpirationTimeout = TimeSpan.FromMinutes(JobExpirationTimeout);
    }

    public void OnStateApplied(Hangfire.States.ApplyStateContext context, IWriteOnlyTransaction transaction)
    {
        var JobExpirationTimeout =  3;
        context.JobExpirationTimeout = TimeSpan.FromMinutes(JobExpirationTimeout);
    }
}
