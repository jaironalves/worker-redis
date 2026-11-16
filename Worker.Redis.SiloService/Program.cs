using Microsoft.AspNetCore.Builder;
using Orleans.Configuration;
using Orleans.Dashboard;
using StackExchange.Redis;
using Worker.Redis.SiloService.HostedServices;
using Worker.Redis.SiloService.Redis;
using Worker.Redis.SiloService.Services;

var builder = WebApplication.CreateBuilder(args);

//var redisConnectionString = builder.Configuration.GetConnectionString("redis")!;
var redisConnectionString = builder.Configuration.GetValue<string>("REDIS_SILO") ?? "localhost:6379";
var redisOptions = ConfigurationOptions.Parse(redisConnectionString);

builder.Services
    .AddSingleton<IConnectionMultiplexer>(sp =>
    {
        var connection = ConnectionMultiplexer.Connect(redisOptions);
        return connection;
    });

builder.UseOrleans(siloBuilder =>
{
    var portaSilo = builder.Configuration.GetValue<int?>("PORTA_SILO") ?? 11111;
    var portaGateway = 30000;

    siloBuilder.Configure<ClusterOptions>(options =>
    {
        options.ClusterId = "appHostCluster";
        options.ServiceId = "appHostService";
    })
    .ConfigureEndpoints(portaSilo, portaGateway, listenOnAnyHostAddress: true);

    //siloBuilder.AddMemoryGrainStorage("")

    siloBuilder.UseRedisClustering(options =>
    {
        options.ConfigurationOptions = redisOptions;
    })
    .AddDashboard();
});

builder.Services.AddSingleton<WorkerState>();

builder.Services.Configure<RedisServiceOptions>(options =>
{
    options.LeaderLeaseDuration = TimeSpan.FromSeconds(15);
    options.LeaderRenewInterval = TimeSpan.FromSeconds(5);
    options.WorkerHeartbeatTtl = TimeSpan.FromSeconds(15);
});

builder.Services.AddSingleton<IRedisService, RedisService>();

builder.Services.AddSingleton<CatalogCoordinator>();

builder.Services.AddSingleton<IAssetCache, AssetCache>();
builder.Services.AddSingleton<ISseOrchestrator, SseOrchestrator>();

builder.Services.AddHostedService<AssetRefreshService>();
builder.Services.AddHostedService<MembershipService>();
builder.Services.AddHostedService<LeaderElectionService>();
builder.Services.AddHostedService<LeaderCoordinatorService>();
builder.Services.AddHostedService<SlotManagerService>();

var app = builder.Build();
app.MapOrleansDashboard();

app.Run();
