using Endor.TinyServices.OData.Common.Entities;
using Endor.TinyServices.OData.Common.Enums;
using Endor.TinyServices.OData.Common.Exceptions;
using Endor.TinyServices.OData.Common.Interfaces;

namespace Endor.TinyServices.OData.Sql.Dialects;

public class SQLDialectConverter : IDialectExpressionConverter
{
	private ODataBuilder _builder;

	public SQLDialectConverter(ODataBuilder builder)
	{
		_builder = builder;
	}

	public string TransformExpression(object data, object context = null)
	{
		if (data is EntityOperatorType)
			return ((EntityOperatorType)data).ToString().ToUpper();

		if (data is PropertyOperatorType)
			return GetPropertyOperatorTypeValue((PropertyOperatorType)data, (IPropertyValue)context);

		if (data is IPropertyValue)
			return GetPropertyValue((IPropertyValue)data);

		throw new ODataParserException($"Unable to process value [{data}]");
	}

	public string TransformFunction(string entityName, string propertyName, FilterFunctionType type, IList<IPropertyValue> parameters)
	{
		switch (type)
		{
			case FilterFunctionType.concat:
				return $"CONCAT({string.Join(',', parameters.Select(x => GetPropertyValue(x)))})";
			case FilterFunctionType.contains:
				return $"CONTAINS({string.Join(',', parameters.Select(x => GetPropertyValue(x)))})";
			case FilterFunctionType.endswith:
				return $"%{string.Join(',', parameters.Select(x => GetPropertyValue(x)))}";
			case FilterFunctionType.indexof:
				break;
			case FilterFunctionType.length:
				return $"LENGTH({string.Join(',', parameters.Select(x => GetPropertyValue(x)))})";
			case FilterFunctionType.startswith:
				return $"{string.Join(',', parameters.Select(x => GetPropertyValue(x)))}%";
			case FilterFunctionType.substring:
				break;
			case FilterFunctionType.matchesPattern:
				break;
			case FilterFunctionType.tolower:
				break;
			case FilterFunctionType.toupper:
				break;
			case FilterFunctionType.trim:
				break;
			case FilterFunctionType.@in:
				return $"{GetEntityWithPropertyName(entityName, propertyName)} IN({string.Join(',', parameters.Select(x => GetPropertyValue(x)))})";
			case FilterFunctionType.day:
				break;
			case FilterFunctionType.date:
				break;
			case FilterFunctionType.fractionalseconds:
				break;
			case FilterFunctionType.hour:
				break;
			case FilterFunctionType.maxdatetime:
				break;
			case FilterFunctionType.mindatetime:
				break;
			case FilterFunctionType.minute:
				break;
			case FilterFunctionType.month:
				break;
			case FilterFunctionType.mow:
				break;
			case FilterFunctionType.second:
				break;
			case FilterFunctionType.time:
				break;
			case FilterFunctionType.totaloffsetminutes:
				break;
			case FilterFunctionType.totalseconds:
				break;
			case FilterFunctionType.year:
				break;
			case FilterFunctionType.ceiling:
				break;
			case FilterFunctionType.floor:
				break;
			case FilterFunctionType.round:
				break;
			default:
				break;
		}

		throw new ODataParserException($"Unable to process function. {type}");

	}

	private object GetEntityWithPropertyName(string entityName, string propertyName)
	{
		if (string.IsNullOrEmpty(entityName)) return propertyName;
		else return $"{entityName}.{propertyName}";
	}

	private string GetPropertyOperatorTypeValue(PropertyOperatorType data, IPropertyValue value)
	{
		switch (data)
		{
			case PropertyOperatorType.none:
				return string.Empty;
			case PropertyOperatorType.eq:
				return value is EntityValue && ((EntityValue)value).Value != "null" ? "=" : "is";
			case PropertyOperatorType.ne:
				return value is EntityValue && ((EntityValue)value).Value != "null" ? "<>" : "is not";
			case PropertyOperatorType.gt:
				return ">";
			case PropertyOperatorType.ge:
				return ">=";
			case PropertyOperatorType.lt:
				return "<";
			case PropertyOperatorType.le:
				return "<=";
			default:
				return string.Empty;
		}
	}

	private string GetPropertyValue(IPropertyValue data)
	{
		if (data is EntityValue)
			return ((EntityValue)data).Value;

		if (data is EntityPropertyPair)
		{
			var item = (EntityPropertyPair)data;

			if (string.IsNullOrEmpty(item.Entity))
				return $"{_builder.GetEntityAlias(_builder.BaseEntityName)}.{item.Property}";
			else
				return $"{_builder.GetEntityAlias(item.Entity)}.{item.Property}";
		}

		throw new ODataParserException($"Unable to parse PropertyValue type [{data}]");
	}

}