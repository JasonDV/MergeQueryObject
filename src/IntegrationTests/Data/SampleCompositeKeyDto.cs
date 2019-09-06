namespace ivaldez.Sql.IntegrationTests.Data
{
    public class SampleCompositeKeyDto
    {
        public int Pk1 { get; set; }
        public string Pk2 { get; set; }
        public string TextValue { get; set; }
        public int? IntValue { get; set; }
        public decimal? DecimalValue { get; set; }
        public bool IsDeleted { get; set; }
    }

    public class SampleCompositeKeyPartialUpdateDto
    {
        public int Pk1 { get; set; }
        public string Pk2 { get; set; }
        public string TextValue { get; set; }
    }
}