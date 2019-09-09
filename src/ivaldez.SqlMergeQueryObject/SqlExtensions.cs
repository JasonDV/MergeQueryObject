using System.Data.SqlClient;

namespace ivaldez.Sql.SqlMergeQueryObject
{
    public static class SqlExtensions {
        public static int Execute(this SqlConnection connection, string sqlText, int? commandTimeout)
        {
            var command = new SqlCommand(sqlText, connection);
            command.CommandTimeout = commandTimeout ?? command.CommandTimeout;

            return command.ExecuteNonQuery();
        }
    }
}