using Endor.TinyServices.OData.Common.Constants;
using Endor.TinyServices.OData.Common.Entities;

namespace Endor.TinyServices.OData.Sql.Builder;

public class SQLMetaParameters : ODataMetaParameters
{
	public string TableName { get; internal set; }

	public override string GetColumnsString()
	{
		return string.Join(',', Properties.Select(c => $"[{Name}].[{c.Key}] {Name}{ODataConventions.ColumnSeparator}{c.Key}"));
	}

}
