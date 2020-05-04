using System.Linq;
using FluentAssertions;
using ivaldez.Sql.IntegrationTests.Data;
using ivaldez.Sql.SqlMergeQueryObject;
using Xunit;

namespace ivaldez.Sql.IntegrationTests.MergeQuery
{
    public class MergeQueryObjectTestsForCombinedActions
    {
        [Fact]
        public void ShouldUpdateOnlyAndDeleteWhenFlaggedInRequest()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateCompositeKeyTable();
            
            helper.DataService.Insert(new SampleCompositeKeyDto
            {
                Pk1 = 1,
                Pk2 = "A",
                TextValue = "JJ",
                IntValue = 100,
                DecimalValue = 100.99m
            });

            helper.DataService.Insert(new SampleCompositeKeyDto
            {
                Pk1 = 2,
                Pk2 = "B",
                TextValue = "ZZ",
                IntValue = 999,
                DecimalValue = 123.45m
            });

            helper.DataService.Insert(new SampleCompositeKeyDto
            {
                Pk1 = 3,
                Pk2 = "B",
                TextValue = "AA",
                IntValue = 1,
                DecimalValue = 1
            });


            var dtos = new[]
            {
                new SampleCompositeKeyDto
                {
                    Pk1 = 1,
                    Pk2 = "A",
                    TextValue = "zz",
                    IntValue = 999,
                    DecimalValue = 999m
                },
                new SampleCompositeKeyDto
                {
                    Pk1 = 4,
                    Pk2 = "C",
                    TextValue = "CC",
                    IntValue = 666,
                    DecimalValue = 666.66m
                }
            };

            var request = new MergeRequest<SampleCompositeKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Pk1, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.Delete,
                OnMergeInsertActive = false
            };

            helper.DataService.Merge(request);

            var sampleDtos = helper.DataService.GetAllSampleDtos<SampleCompositeKeyDto>().ToArray();

            sampleDtos.Length.Should().Be(1);

            var firstDto = sampleDtos.First(x => x.Pk1 == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.TextValue.Should().Be("zz");
            firstDto.IntValue.Should().Be(999);
            firstDto.DecimalValue.Should().Be(999);
        }
    }
}