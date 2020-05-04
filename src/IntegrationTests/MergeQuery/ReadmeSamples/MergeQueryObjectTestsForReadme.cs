using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using ivaldez.Sql.IntegrationTests.Data;
using ivaldez.Sql.SqlMergeQueryObject;
using Xunit;

namespace ivaldez.Sql.IntegrationTests.MergeQuery.ReadmeSamples
{
    public class MergeQueryObjectTestsForReadme
    {
        [Fact]
        public void BasicUsageSimpleMerge()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateSingleSurrogateKeyTable();
            
            helper.DataService.Insert(new SampleSurrogateKey
            {
                Pk = 1,
                TextValue = "JJ",
                IntValue = 1,
                DecimalValue = 1
            });

            var dtos = new[]
            {
                new SampleSurrogateKey
                {
                    Pk = 1,
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleSurrogateKey
                {
                    Pk = 0,
                    TextValue = "BB",
                    IntValue = 200,
                    DecimalValue = 200.99m
                }
            };

            var request = new MergeRequest<SampleSurrogateKey>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Pk},
                KeepPrimaryKeyInInsertStatement = false
            };

            helper.DataService.Merge(request);

            var sampleDtos = helper
                .DataService
                .GetAllSampleDtos<SampleSurrogateKey>()
                .ToArray();

            sampleDtos.Length.Should().Be(2);

            var firstDto = sampleDtos.First(x => x.TextValue == "JJ");
            firstDto.Pk.Should().BeGreaterThan(0);
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = sampleDtos.First(x => x.TextValue == "BB");
            secondDto.Pk.Should().BeGreaterThan(0);
            secondDto.IntValue.Should().Be(200);
            secondDto.DecimalValue.Should().Be(200.99m);
        }

        [Fact]
        public void BasicUsageControllingBulkLoadOperation()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateSingleSurrogateKeyTable();
            
            helper.DataService.Insert(new SampleSurrogateKey
            {
                Pk = 1,
                TextValue = "JJ",
                IntValue = 1,
                DecimalValue = 1
            });

            var dtos = new[]
            {
                new SampleSurrogateKeyDifferentNamesDto
                {
                    Pk = 1,
                    TextValueExtra  = "JJ",
                    IntValueExtra  = 100,
                    DecimalValueExtra  = 100.99m
                },
                new SampleSurrogateKeyDifferentNamesDto
                {
                    Pk = 0,
                    TextValueExtra  = "BB",
                    IntValueExtra  = 200,
                    DecimalValueExtra  = 200.99m
                }
            };

            var request = new MergeRequest<SampleSurrogateKeyDifferentNamesDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Pk},
                KeepPrimaryKeyInInsertStatement = false,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.Delete,
                OnMergeUpdateActive = true,
                OnMergeInsertActive = true,
                BulkLoaderOptions =
                    t => t.With(c => c.TextValueExtra, "TextValue")
                        .With(c => c.IntValueExtra, "IntValue")
                        .With(c => c.DecimalValueExtra, "DecimalValue")
            };

            helper.DataService.Merge(request);

            var sampleDtos = helper
                .DataService
                .GetAllSampleDtos<SampleSurrogateKey>()
                .ToArray();

            sampleDtos.Length.Should().Be(2);

            var firstDto = sampleDtos.First(x => x.TextValue == "JJ");
            firstDto.Pk.Should().BeGreaterThan(0);
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = sampleDtos.First(x => x.TextValue == "BB");
            secondDto.Pk.Should().BeGreaterThan(0);
            secondDto.IntValue.Should().Be(200);
            secondDto.DecimalValue.Should().Be(200.99m);
        }
    }
}
