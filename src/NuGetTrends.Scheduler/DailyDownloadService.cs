using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using NuGet.Protocol.Core.Types;
using NuGetTrends.Data;

namespace NuGetTrends.Scheduler
{
    public interface IDailyDownloadService
    {
        Task UpdateDownloadCount(IList<string> packageIds, CancellationToken cancellationToken);
    }

    public class DailyDownloadService : IDailyDownloadService
    {
        private readonly INuGetSearchService _nuGetSearchService;
        private readonly IServiceProvider _services;
        private readonly ILogger<DailyDownloadService> _logger;

        public DailyDownloadService(
            INuGetSearchService nuGetSearchService,
            IServiceProvider services,
            ILogger<DailyDownloadService> logger)
        {
            _nuGetSearchService = nuGetSearchService;
            _services = services;
            _logger = logger;
        }

        public async Task UpdateDownloadCount(IList<string> packageIds, CancellationToken cancellationToken)
        {
            var tasks = new List<Task<IPackageSearchMetadata?>>(packageIds.Count);
            foreach (var id in packageIds)
            {
                tasks.Add(_nuGetSearchService.GetPackage(id, cancellationToken));
            }

            var whenAll = Task.WhenAll(tasks);
            try
            {
                await whenAll;
            }
            catch when (whenAll.Exception is {} exs && exs.InnerExceptions.Count > 1)
            {
                throw exs; // re-throw the AggregateException to capture all errors with Sentry
            }

            using var scope = _services.CreateScope();
            using var context = scope.ServiceProvider.GetRequiredService<NuGetTrendsContext>();
            for (var i = 0; i < tasks.Count; i++)
            {
                var expectedPackageId = packageIds[i];
                var packageMetadata = tasks[i].Result;
                if (packageMetadata == null)
                {
                    // All versions are unlisted or:
                    // "This package has been deleted from the gallery. It is no longer available for install/restore."
                    _logger.LogInformation("Package deleted: {packageId}", expectedPackageId);
                    await RemovePackage(context, expectedPackageId, cancellationToken);
                }
                else
                {
                    context.DailyDownloads.Add(new DailyDownload
                    {
                        PackageId = packageMetadata.Identity.Id,
                        Date = DateTime.UtcNow.Date,
                        DownloadCount = packageMetadata.DownloadCount
                    });

                    void Update(PackageDownload package, IPackageSearchMetadata metadata)
                    {
                        if (metadata.IconUrl?.ToString() is { } url)
                        {
                            package.IconUrl = url;
                        }
                        package.LatestDownloadCount = metadata.DownloadCount;
                        package.LatestDownloadCountCheckedUtc = DateTime.UtcNow;
                    }

                    var pkgDownload = await context.PackageDownloads.FirstOrDefaultAsync(
                        p => p.PackageIdLowered == packageMetadata.Identity.Id.ToLower(),
                        cancellationToken: cancellationToken);

                    if (pkgDownload == null)
                    {
                        pkgDownload = new PackageDownload
                        {
                            PackageId = packageMetadata.Identity.Id,
                            PackageIdLowered = packageMetadata.Identity.Id.ToLower(),
                        };
                        Update(pkgDownload, packageMetadata);
                        context.PackageDownloads.Add(pkgDownload);
                    }
                    else
                    {
                        Update(pkgDownload, packageMetadata);
                        context.PackageDownloads.Update(pkgDownload);
                    }
                }

                try
                {
                    await context.SaveChangesAsync(cancellationToken);
                }
                catch (DbUpdateException e)
                    when (e.InnerException is PostgresException pge
                          && (pge.ConstraintName == "PK_daily_downloads"))
                {
                    // Re-entrancy
                    _logger.LogWarning(e, "Skipping record already tracked.");
                }
            }
        }

        private async Task RemovePackage(NuGetTrendsContext context, string packageId, CancellationToken token)
        {
            var package = await context.PackageDetailsCatalogLeafs.Where(p => p.PackageId == packageId)
                .ToListAsync(token)
                .ConfigureAwait(false);

            if (package.Count == 0)
            {
                _logger.LogError("Package with Id {packageId} not found!.", packageId);
                return;
            }

            context.PackageDetailsCatalogLeafs.RemoveRange(package);
        }
    }
}
