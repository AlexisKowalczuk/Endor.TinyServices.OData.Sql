using Endor.TinyServices.OData.Interfaces;
using Microsoft.Extensions.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace Endor.TinyServices.OData.Sql;

public class DataAccess : IDataAccess
{
	private string _connectionString;

	public DataAccess(string connectionName, IConfiguration configuration)
	{
		_connectionString = configuration.GetConnectionString(connectionName);
	}

	public async Task<DataTable> ExcecuteQuery(string query)
	{
		using var connection = new SqlConnection(_connectionString);

		connection.Open();

		using var command = new SqlCommand(query, connection);

		var reader = command.ExecuteReader();

		DataTable dt = new DataTable();
		dt.Clear();

		var columns = new List<string>();

		for (int i = 0; i < reader.FieldCount; i++)
		{
			dt.Columns.Add(reader.GetName(i));
		}

		while (reader.Read())
		{
			DataRow row = dt.NewRow();

			for (int i = 0; i < reader.FieldCount; i++)
			{
				row[dt.Columns[i]] = reader[i];
			}
			dt.Rows.Add(row);
		}

		return dt;

	}

}
