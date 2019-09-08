using System.Linq;
using FluentAssertions;
using ivaldez.Sql.IntegrationTests.Data;
using ivaldez.Sql.SqlMergeQueryObject;
using Xunit;

namespace ivaldez.Sql.IntegrationTests.MergeQuery
{
    public class MergeQueryObjectTestsForRequestOptions
    {
        [Fact]
        public void ShouldCreateSampleTable()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKey
                {
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleSurrogateKey
                {
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleSurrogateKey>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = true,
                PrimaryKeyExpression = t => new object[] {t.Pk},
                KeepPrimaryKeyInInsertStatement = false
            };

            helper.DataService.Merge(request);

            var sourceDtos = helper.DataService.GetAllSampleSurrogateKey().ToArray();

            var firstDto = sourceDtos.First(x => x.TextValue == "JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = sourceDtos.First(x => x.TextValue == "ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }


        [Fact]
        public void ShouldKeepIdentityColumnsWhenFlagged()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKey
                {
                    Pk = 100,
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleSurrogateKey
                {
                    Pk = 200,
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleSurrogateKey>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Pk},
                KeepPrimaryKeyInInsertStatement = true,
                KeepIdentityColumnValueOnInsert = true
            };

            helper.DataService.Merge(request);

            var sourceDtos = helper.DataService.GetAllSampleSurrogateKey().ToArray();

            var firstDto = sourceDtos.First(x => x.Pk == 100);
            firstDto.TextValue.Should().Be("JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = sourceDtos.First(x => x.Pk == 200);
            secondDto.TextValue.Should().Be("ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }


        [Fact]
        public void ShouldMergeDtoWithDerivedColumns()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKeyWithDerivedColumns
                {
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleSurrogateKeyWithDerivedColumns
                {
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleSurrogateKeyWithDerivedColumns>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Pk},
                ColumnsToExcludeExpressionOnUpdate = t => new object[] {t.ExtraColumn},
                ColumnsToExcludeExpressionOnInsert = t => new object[] {t.ExtraColumn},
                KeepPrimaryKeyInInsertStatement = false
            };

            helper.DataService.Merge(request);

            var sourceDtos = helper.DataService.GetAllSampleSurrogateKey().ToArray();

            var firstDto = sourceDtos.First(x => x.TextValue == "JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = sourceDtos.First(x => x.TextValue == "ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }
    }
}