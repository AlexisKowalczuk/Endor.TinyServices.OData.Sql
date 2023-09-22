using Endor.TinyServices.OData.Common.Constants;
using Endor.TinyServices.OData.Common.Entities;
using Endor.TinyServices.OData.Common.Enums;
using Endor.TinyServices.OData.Common.Exceptions;
using Endor.TinyServices.OData.Common.Helper;
using Endor.TinyServices.OData.Interfaces.Dialect;
using Endor.TinyServices.OData.Interfaces.Schema;
using Endor.TinyServices.OData.Parser;
using Endor.TinyServices.OData.Sql.Builder;
using System.Reflection;

namespace Endor.TinyServices.OData.Sql.Dialects;

public class SQLDialect : IQueryDialect
{
	private IMetadataProvider _provider;

	public SQLDialect(IMetadataProvider provider)
	{
		_provider = provider;
	}

	public async Task<ODataBuilder> Init(string entityName, string tenantId = null)
	{
		var item = await _provider.GetEntity(entityName);
		if (item == null) throw new EntityNotFoundException(entityName);

		var builder = new SQLQueryBuilder();
		builder.BaseEntityName = entityName;
		builder.TenantId = tenantId;
		builder.TableName = AttributeHelper.GetTableNameFromEntity(item);
		var tableName = builder.TableName;

		if (builder.TenantId != null) tableName = $"[{builder.TenantId}].[{tableName}]";

		var tableAlias = builder.GetNewEntityIndex();

		var properties = item.GetProperties().Where(i => i.PropertyType.Namespace == "System" || i.PropertyType.IsEnum).ToList();
		builder.AddMetadata(entityName, new SQLMetaParameters() { Entity = item, Name = tableAlias, Properties = properties.ToDictionary(x => x.Name) });

		builder.SetQuery($"{builder.ToString()}SELECT {builder.GetColumnsString()} FROM {tableName} AS [{tableAlias}]");
		return builder;
	}

	public async Task TopStatement(int number, ODataBuilder builder)
	{
		var key = "SELECT";
		var keyIndex = builder.ToString().IndexOf(key) + key.Length;

		builder.SetQuery($"{builder.ToString().Substring(0, keyIndex)} TOP ({number}) {builder.ToString().Substring(keyIndex, builder.ToString().Length - keyIndex)}");
	}

	public async Task SkipStatement(int number, ODataBuilder builder)
	{
		var key = "SELECT";
		var keyIndex = builder.ToString().IndexOf(key) + key.Length;

		//REMOVE RONUM FROM QUERY AFTER EXCEUTION
		//CHANGE "ORDER BY ID" TO CUSTOM IDENTIFIER USING FLUENT CONFIG
		var item = await _provider.GetEntity(builder.BaseEntityName);
		var refEntityId = AttributeHelper.GetIdForEntity(item);

		var queryString = @$"
					DECLARE @N INT = {number}
					SELECT *
					FROM (
								{builder.ToString().Substring(0, keyIndex)}
								ROW_NUMBER() OVER(ORDER BY [{builder.GetBaseMetaParameter().Name}].[{refEntityId}]) AS RoNum,
								{builder.ToString().Substring(keyIndex, builder.ToString().Length - keyIndex)}
							) AS tbl
					WHERE @N < tbl.RoNum
					";

		builder.SetQuery(queryString);

	}

	public async Task ExpandStatement(string entityName, IList<(QueryTypeParameter, string)> additionalInfo, ODataBuilder builder)
	{
		var item = await _provider.GetEntity(entityName);
		if (item == null) throw new EntityNotFoundException(entityName);

		var baseEntityProperties = (await _provider.GetEntity(builder.BaseEntityName)).GetProperties();
		var refAttribute = AttributeHelper.GetReferenceAttribute(await _provider.GetEntity(builder.BaseEntityName), entityName);
		var refEntityId = AttributeHelper.GetIdForEntity(item);

		var properties = item.GetProperties().Where(i => i.PropertyType.Namespace == "System" || i.PropertyType.IsEnum).ToList();
		var tableAlias = builder.GetNewEntityIndex();
		var tableName = AttributeHelper.GetTableNameFromEntity(item);

		if (builder.TenantId != null) tableName = $"[{builder.TenantId}].[{tableName}]";

		var meta = new SQLMetaParameters() { Entity = item, Name = tableAlias, TableName = tableName, Properties = properties.ToDictionary(x => x.Name) };
		
		SQLMetaParameters parameter = (SQLMetaParameters)builder.GetBaseMetaParameter();

		var rawQuery = builder.ToString();

		var from = "FROM";
		var fromIndex = rawQuery.IndexOf(from);

		var beforeFrom = rawQuery.Substring(0, fromIndex);
		var afterFrom = rawQuery.Substring(fromIndex, rawQuery.Length - fromIndex);

		var tableAliasString = $"[{parameter.Name}]";
		var afterTableAliasStringIndex = afterFrom.IndexOf(tableAliasString) + tableAliasString.Length;

		var beforeJoinStatement = afterFrom.Substring(0, afterTableAliasStringIndex);
		var afterJoinStatement = afterFrom.Substring(afterTableAliasStringIndex, afterFrom.Length - afterTableAliasStringIndex);

		var newQuery = @$"{beforeFrom},{meta.GetColumnsString()} 
					{beforeJoinStatement}
					LEFT JOIN {tableName} AS [{tableAlias}] ON [{tableAlias}].[{refEntityId}] = [{parameter.Name}].[{refAttribute.Name}]
					{afterJoinStatement}";

		builder.AddMetadata(entityName, meta);
		builder.SetQuery(newQuery);

		await ProcessAdditionalInfo(entityName, additionalInfo, builder);

		if (additionalInfo != null)
		{
			foreach (var expandAdd in additionalInfo.Where(x => x.Item1 == QueryTypeParameter.Expand))
			{
				await this.ExpandStatement(expandAdd.Item2, null, builder);
			}
		}
	}

	private async Task ProcessAdditionalInfo(string entityName, IList<(QueryTypeParameter, string)> additionalInfo, ODataBuilder builder)
	{
		if (additionalInfo == null || additionalInfo.Any(x => x.Item1 != QueryTypeParameter.Select)) return;

		//if (additionalInfo.Any(x => x.Item1 != QueryTypeParameter.Select)) throw new ODataParserException($"The operators [{string.Join(',', additionalInfo.Where(x => x.Item1 != QueryTypeParameter.Select).Select(x => x.Item1.ToString()))}] are not supported on Expand statement yet.");

		var properties = additionalInfo.FirstOrDefault(x => x.Item1 == QueryTypeParameter.Select).Item2;

		await SelectStatement(properties.Split(','), entityName, builder);
	}

	public async Task FilterStatement(string filter, ODataBuilder builder)
	{
		var data = filter.ToString();
		var parser = new ODataParser();
		var result = parser.Parse(ref data);

		var filterStatement = result.Read(new SQLDialectConverter(builder));

		var rawQuery = builder.ToString();
		rawQuery += " WHERE " + filterStatement;

		builder.SetQuery(rawQuery);
	}

	public async Task OrderByStatement(string property, bool asc, bool thenBy, ODataBuilder builder)
	{
		var entity = builder.BaseEntityName;

		if (property.Contains('/'))
		{
			var rawData = property.Split('/');
			entity = rawData[0];
			property = rawData[1];
		}

		if (!builder.ExistsPropertyMeta(entity, property))
		{
			if (!await _provider.ExistsProperty(entity, property))
				throw new ODataParserException($"Unable to get property [{property}] from entity [{entity}] over order by clause");
			else
				throw new ODataParserException($"The property [{property}] must be include into select statement to be used over order by clause");
		}

		var query = builder.ToString();

		var ascString = asc ? "ASC" : "DESC";
		var preOrderStatement = thenBy ? "," : "ORDER BY";
		query += $"{preOrderStatement} {builder.GetEntityAlias(entity)}_{property} {ascString}";

		builder.SetQuery(query);
	}

	public async Task SelectStatement(string[] propNames, string entity, ODataBuilder builder)
	{
		var metaProperties = builder.GetMetaProperties(entity);
		var alias = builder.GetEntityAlias(entity);
		var propUnkwons = propNames.Where(x => !metaProperties.Any(p => p.Value.Name == x));
		if (propUnkwons.Any()) throw new ODataParserException($"Unable to process select statement. The main entity has no contains definition of [{string.Join(',', propUnkwons)}]");

		var itemsToRemove = metaProperties.Where(x => !propNames.Any(name => name == x.Key)).Select(x => x.Key).ToList();

		var query = builder.ToString();
		foreach (var item in itemsToRemove)
		{
			var selectStatement = $"[{alias}].[{item}] {alias}_{item}";
			var index = query.IndexOf(selectStatement);
			if (index == -1) throw new ODataParserException($"Unable to find selected property on query.");

			query = query.Remove(index, selectStatement.Length);

			while (!char.IsLetter(query[index]) && query[index] != ',' && index > 0) { index--; }

			if (query[index] == ',') query = query.Remove(index, 1);

			builder.RemoveParameter(item, entity);
		}

		builder.SetQuery(query);
	}

}
