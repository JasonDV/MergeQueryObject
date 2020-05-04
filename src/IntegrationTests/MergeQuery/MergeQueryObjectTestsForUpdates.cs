using System.Linq;
using FluentAssertions;
using ivaldez.Sql.IntegrationTests.Data;
using ivaldez.Sql.SqlMergeQueryObject;
using Xunit;

namespace ivaldez.Sql.IntegrationTests.MergeQuery
{
    public class MergeQueryObjectTestsForUpdates
    {
        [Fact]
        public void ShouldAllowForPartialUpdateWithPartialObject()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateCompositeKeyTable();

            var dtos = new[]
            {
                new SampleCompositeKeyDto
                {
                    Pk1 = 1,
                    Pk2 = "A",
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                }
            };

            var request = new MergeRequest<SampleCompositeKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Pk1, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.Delete
            };

            helper.DataService.Merge(request);

            var sampleDtos = helper.DataService.GetAllSampleDtos<SampleCompositeKeyDto>().ToArray();

            var firstDto = sampleDtos.First(x => x.Pk1 == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.TextValue.Should().Be("JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);
            firstDto.IsDeleted.Should().BeFalse();

            //**

            var dtos2 = new[]
            {
                new SampleCompositeKeyPartialUpdateDto
                {
                    Pk1 = 1,
                    Pk2 = "A",
                    TextValue = "JJ_Partial"
                }
            };


            var request2 = new MergeRequest<SampleCompositeKeyPartialUpdateDto>
            {
                DataToMerge = dtos2,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Pk1, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.None,
                OnMergeUpdateActive = true
            };

            helper.DataService.Merge(request2);

            var sampleDtos2 = helper.DataService.GetAllSampleDtos<SampleCompositeKeyDto>().ToArray();

            var firstDto2 = sampleDtos2.First(x => x.Pk1 == 1);
            firstDto2.Pk2.Should().Be("A");
            firstDto2.TextValue.Should().Be("JJ_Partial");
            firstDto2.IntValue.Should().Be(100);
            firstDto2.DecimalValue.Should().Be(100.99m);
            firstDto2.IsDeleted.Should().BeFalse();
        }


        [Fact]
        public void ShouldIncludePrimaryKeyInUpdate()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateCompositeKeyTable();

            var dtos = new[]
            {
                new SampleCompositeKeyDto
                {
                    Pk1 = 1,
                    Pk2 = "A",
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleCompositeKeyDto
                {
                    Pk1 = 2,
                    Pk2 = "B",
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleCompositeKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Pk1, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true
            };

            helper.DataService.Merge(request);

            var sampleDtos = helper.DataService.GetAllSampleDtos<SampleCompositeKeyDto>().ToArray();

            var firstDto = sampleDtos.First(x => x.Pk1 == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.TextValue.Should().Be("JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = sampleDtos.First(x => x.Pk1 == 2);
            secondDto.Pk2.Should().Be("B");
            secondDto.TextValue.Should().Be("ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }

        [Fact]
        public void ShouldUpdateFieldsWithMerge()
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
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Pk},
                KeepPrimaryKeyInInsertStatement = false
            };

            helper.DataService.Merge(request);

            var firstSetOfInsertedDtos = helper.DataService.GetAllSampleDtos<SampleSurrogateKey>().ToArray();

            var firstInsertDto = firstSetOfInsertedDtos.First(x => x.TextValue == "JJ");
            firstInsertDto.IntValue.Should().Be(100);
            firstInsertDto.DecimalValue.Should().Be(100.99m);

            var secondInsertDto = firstSetOfInsertedDtos.First(x => x.TextValue == "ZZ");
            secondInsertDto.IntValue.Should().Be(999);
            secondInsertDto.DecimalValue.Should().Be(123.45m);

            firstInsertDto.TextValue = "1";
            firstInsertDto.IntValue = 2;
            firstInsertDto.DecimalValue = 3;

            secondInsertDto.TextValue = "2";
            secondInsertDto.IntValue = 3;
            secondInsertDto.DecimalValue = 4;

            request.DataToMerge = firstSetOfInsertedDtos;

            helper.DataService.Merge(request);

            var secondSetOfUpdateDtos = helper.DataService.GetAllSampleDtos<SampleSurrogateKey>().ToArray();

            var firstUpdateDto = secondSetOfUpdateDtos.First(x => x.TextValue == "1");
            firstUpdateDto.IntValue.Should().Be(2);
            firstUpdateDto.DecimalValue.Should().Be(3);

            var secondUpdateDto = secondSetOfUpdateDtos.First(x => x.TextValue == "2");
            secondUpdateDto.IntValue.Should().Be(3);
            secondUpdateDto.DecimalValue.Should().Be(4);
        }

        [Fact]
        public void ShouldUpdateOnlyWhenFlagged()
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
                }
            };

            var request = new MergeRequest<SampleCompositeKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Pk1, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.None,
                OnMergeUpdateActive = true
            };

            helper.DataService.Merge(request);

            var sourceDtos = helper.DataService.GetAllSampleDtos<SampleCompositeKeyDto>().ToArray();

            var firstDto = sourceDtos.First(x => x.Pk1 == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.TextValue.Should().Be("zz");
            firstDto.IntValue.Should().Be(999);
            firstDto.DecimalValue.Should().Be(999);

            var secondDto = sourceDtos.First(x => x.Pk1 == 2);
            secondDto.Pk2.Should().Be("B");
            secondDto.TextValue.Should().Be("ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);

            var thirdDto = sourceDtos.First(x => x.Pk1 == 3);
            thirdDto.Pk2.Should().Be("B");
            thirdDto.TextValue.Should().Be("AA");
            thirdDto.IntValue.Should().Be(1);
            thirdDto.DecimalValue.Should().Be(1);
        }
    }
}