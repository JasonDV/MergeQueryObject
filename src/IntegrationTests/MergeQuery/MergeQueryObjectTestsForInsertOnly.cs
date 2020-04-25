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
    public class MergeQueryObjectTestsForInsertOnly
    {
        [Fact]
        public void ShouldUpdateOnlyWhenFlagged()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateSingleSurrogateKeyTable();

            helper.DataService.Insert(new SampleSurrogateKey()
            {
                Pk = 1,
                TextValue = "JJ",
                IntValue = 100,
                DecimalValue = 100.99m
            });

            helper.DataService.Insert(new SampleSurrogateKey
            {
                Pk = 2,
                TextValue = "ZZ",
                IntValue = 999,
                DecimalValue = 123.45m
            });

            helper.DataService.Insert(new SampleSurrogateKey
            {
                Pk = 3,
                TextValue = "AA",
                IntValue = 1,
                DecimalValue = 1
            });


            var dtos = new[]
            {
                new SampleSurrogateKey
                {
                    Pk = 1,
                    TextValue = "zz",
                    IntValue = 0,
                    DecimalValue = 0
                },
                new SampleSurrogateKey
                {
                    Pk = 2,
                    TextValue = "zz",
                    IntValue = 0,
                    DecimalValue = 0
                },
                new SampleSurrogateKey
                {
                    Pk = 5,
                    TextValue = "INSERT",
                    IntValue = 99,
                    DecimalValue = 99.1m
                }
            };

            var request = new MergeRequest<SampleSurrogateKey>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] { t.Pk },
                KeepIdentityColumnValueOnInsert = true,
                KeepPrimaryKeyInInsertStatement = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.None,
                OnMergeUpdateActive = false,
                OnMergeInsertActive = true
            };

            helper.DataService.Merge(request);

            var sourceDtos = helper.DataService.GetAllSampleSurrogateKey().ToArray();

            var firstDto = sourceDtos.First(x => x.Pk == 1);
            firstDto.TextValue.Should().Be("JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = sourceDtos.First(x => x.Pk == 2);
            secondDto.TextValue.Should().Be("ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);

            var thirdDto = sourceDtos.First(x => x.TextValue == "INSERT");
            thirdDto.IntValue.Should().Be(99);
            thirdDto.DecimalValue.Should().Be(99.1m);
        }
    }
}
