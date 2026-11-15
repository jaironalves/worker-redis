var builder = DistributedApplication.CreateBuilder(args);

//var redisPassword
//    = builder.AddParameter("redis-password", () => "123456", false, false);

//var redis = builder.AddRedis("redis", password: redisPassword);

//var myService = builder.AddProject<Projects.AppHost2_WorkerService1>("worker")
//                       .WithReplicas(3)
//                       .WithReference(redis)
//                       .WaitForStart(redis);

//var siloService = builder.AddProject<Projects.AppHost2_SiloService>("silo")
//    .WithHttpEndpoint()
//    .WithReplicas(3)
//    .WithReference(redis)
//    .WaitForStart(redis);

var siloService01 = builder.AddProject<Projects.Worker_Redis_SiloService>("silo-01")
    .WithEnvironment("PORTA_SILO", "11111")
    .WithEnvironment("REDIS_SILO", "localhost:6379")
    .WithHttpEndpoint();
//.WithReference(redis)
//.WaitForStart(redis);


var siloService02 = builder.AddProject<Projects.Worker_Redis_SiloService>("silo-02")
    .WithEnvironment("PORTA_SILO", "11112")
    .WithEnvironment("REDIS_SILO", "localhost:6379")
    .WithHttpEndpoint();
//.WithReference(redis)
//.WaitForStart(redis);

var siloService03 = builder.AddProject<Projects.Worker_Redis_SiloService>("silo-03")
    .WithEnvironment("PORTA_SILO", "11113")
    .WithEnvironment("REDIS_SILO", "localhost:6379")
    .WithHttpEndpoint();
    //.WithReference(redis)
    //.WaitForStart(redis);

builder.Build().Run();
