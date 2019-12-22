using System;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NuGet.Packaging.Core;
using NuGet.Protocol.Core.Types;
using NuGet.Versioning;
using NuGetTrends.Data;
using Xunit;
using static System.Threading.CancellationToken;

namespace NuGetTrends.Scheduler.Tests
{
    public class DailyDownloadServiceTests
    {
        [Fact]
        public async Task Testa()
        {
            var packageId = "Sentry";
            var nuGetSearchService = Substitute.For<INuGetSearchService>();
            var packageSearchMetadata = Substitute.For<IPackageSearchMetadata>();
            var packageIdentity = new PackageIdentity(packageId, NuGetVersion.Parse("1.0.0"));
            packageSearchMetadata.Identity.Returns(packageIdentity);
            var nuGetTrendsContext = new NuGetTrendsContext(new DbContextOptions<NuGetTrendsContext>());
            nuGetSearchService.GetPackage(packageId, None).Returns(Task.FromResult(packageSearchMetadata));
            var serviceProvider = Substitute.For<IServiceProvider>();
            serviceProvider.GetService(typeof(NuGetTrendsContext)).Returns(nuGetTrendsContext);
            var serviceScopeFactory = Substitute.For<IServiceScopeFactory>();
            serviceProvider.GetService(typeof(IServiceScopeFactory)).Returns(serviceScopeFactory);
            var serviceScope = Substitute.For<IServiceScope>();
            serviceScope.ServiceProvider.Returns(serviceProvider);
            serviceScopeFactory.CreateScope().Returns(serviceScope);
            var logger = Substitute.For<ILogger<DailyDownloadService>>();
            var sut = new DailyDownloadService(nuGetSearchService, serviceProvider, logger);
            var ids = new[] {packageId};
            await sut.UpdateDownloadCount(ids, None);
        }
    }
}
