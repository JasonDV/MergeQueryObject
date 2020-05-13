using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using ivaldez.Sql.IntegrationTests.Data;
using ivaldez.Sql.SqlMergeQueryObject;
using Xunit;

namespace ivaldez.Sql.IntegrationTests.MergeQuery
{
    public class MergeQueryObjectTestsForCustomExceptionHandling
    {
            [Fact]
        public void ShouldMergeWhenBulkLoadUsesRenamedPrimaryKey()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKeyDifferentNamePrimaryKeyDto
                {
                    PkPrimaryKey = 100,
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleSurrogateKeyDifferentNamePrimaryKeyDto
                {
                    PkPrimaryKey = 200,
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleSurrogateKeyDifferentNamePrimaryKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.PkPrimaryKey},
                KeepPrimaryKeyInInsertStatement = true,
                KeepIdentityColumnValueOnInsert = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.None,
                OnMergeUpdateActive = false,
                BulkLoaderOptions =
                    t => t.With(c => c.PkPrimaryKey, "Pk")
            };

            helper.DataService.Merge(request);

            var sourceDtos = helper.DataService.GetAllSampleDtos<SampleSurrogateKey>().ToArray();

            var firstDto = sourceDtos.First(x => x.TextValue == "JJ");
            firstDto.Pk.Should().Be(100);
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = sourceDtos.First(x => x.TextValue == "ZZ");
            secondDto.Pk.Should().Be(200);
            secondDto.Pk.Should().BeGreaterThan(0);
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }
    }
}
