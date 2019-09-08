using System.Linq;
using FluentAssertions;
using ivaldez.Sql.IntegrationTests.Data;
using ivaldez.Sql.SqlMergeQueryObject;
using Xunit;

namespace ivaldez.Sql.IntegrationTests.MergeQuery
{
    public class MergeQueryObjectTestsForRemappingBehavior
    {
        [Fact]
        public void ShouldMergeWhenBulkLoadUsesRenamedFields()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKeyDifferentNamesDto
                {
                    Pk = 100,
                    TextValueExtra = "JJ",
                    IntValueExtra = 100,
                    DecimalValueExtra = 100.99m
                },
                new SampleSurrogateKeyDifferentNamesDto
                {
                    Pk = 200,
                    TextValueExtra = "ZZ",
                    IntValueExtra = 999,
                    DecimalValueExtra = 123.45m
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
                OnMergeUpdateOnly = false,
                BulkLoaderOptions =
                    t => t.With(c => c.TextValueExtra, "TextValue")
                        .With(c => c.IntValueExtra, "IntValue")
                        .With(c => c.DecimalValueExtra, "DecimalValue")
            };

            helper.DataService.Merge(request);

            var sourceDtos = helper.DataService.GetAllSampleSurrogateKey().ToArray();

            var firstDto = sourceDtos.First(x => x.TextValue == "JJ");
            firstDto.Pk.Should().BeGreaterThan(0);
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = sourceDtos.First(x => x.TextValue == "ZZ");
            secondDto.Pk.Should().BeGreaterThan(0);
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }
    }
}