using Figgle;
using GalacticArchive;
using GalacticArchive.IndexingEngine;

Console.WriteLine();
Console.WriteLine(FiggleFonts.Standard.Render("GALACTIC ARCHIVE"));

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        services.AddHostedService<GalacticWorker>();
        services.AddTransient<GalacticSqlServerIndexer>();

        services.Configure<GalacticOptions>(o =>
        {
            o.ConnectionString = hostContext.Configuration.GetConnectionString("DefaultConnection");
        });
    })
    .Build();

await host.RunAsync();
