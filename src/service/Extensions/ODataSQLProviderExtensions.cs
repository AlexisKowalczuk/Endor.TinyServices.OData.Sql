using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Endor.TinyServices.OData.Sql.Dialects;
using Endor.TinyServices.OData.Interfaces.Boopstrapper;
using Endor.TinyServices.OData.Interfaces;
using Endor.TinyServices.OData.Interfaces.Dialect;

namespace Endor.TinyServices.OData.Sql.Extensions;

public static class ODataSQLProviderExtensions
{

	public static IODataInterpreterConfigService UseSql(this IODataInterpreterConfigService provider, string connectionString)
	{
		provider.Services.AddScoped<IDataAccess>(c => new DataAccess(connectionString, c.GetService<IConfiguration>()));
		provider.Services.AddScoped<IQueryDialect, SQLDialect>();
		return provider;
	}

}