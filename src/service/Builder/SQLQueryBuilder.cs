using Endor.TinyServices.OData.Common.Entities;

namespace Endor.TinyServices.OData.Sql.Builder;

public class SQLQueryBuilder : ODataBuilder
{
	public string TableName { get; internal set; }

	public override string GetNewEntityIndex() => $"T{_metadata.Count}";

	public override string GetColumnsString()
	{
		var result = new List<string>();
		foreach (var item in _metadata)
		{
			result.Add(item.Value.GetColumnsString());
		}

		return string.Join(',', result);
	}

}
