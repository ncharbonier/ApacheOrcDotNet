using ApacheOrcDotNet.ColumnTypes;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Stripes;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace ApacheOrcDotNet
{
    public class OrcReader
    {
        readonly Type _type;
        readonly FileTail _fileTail;
        readonly bool _ignoreMissingColumns;

        public OrcReader(Type type, System.IO.Stream inputStream, bool ignoreMissingColumns = false)
        {
            _type = type;
            _ignoreMissingColumns = ignoreMissingColumns;
            _fileTail = new FileTail(inputStream);

            if (_fileTail.Footer.Types[0].Kind != Protocol.ColumnTypeKind.Struct)
                throw new InvalidDataException($"The base type must be {nameof(Protocol.ColumnTypeKind.Struct)}");
        }

        public IEnumerable<object> Read()
        {
            var properties = FindColumnsForType(_type, _fileTail.Footer).ToList();

            foreach (var stripe in _fileTail.Stripes)
            {
                var stripeStreams = stripe.GetStripeStreamCollection();
                var readAndSetters = properties.Select(p => GetReadAndSetterForColumn(p.propertyInfo, stripeStreams, p.columnId, p.columnType)).ToList();

                for (ulong i = 0; i < stripe.NumRows; i++)
                {
                    var obj = Activator.CreateInstance(_type);
                    foreach (var readAndSetter in readAndSetters)
                    {
                        readAndSetter(obj);
                    }
                    yield return obj;
                }
            }
        }

        IEnumerable<object> ReadSubType(Type type, uint columnId)
        {
            var properties = FindColumnsForType(type, _fileTail.Footer, (int)columnId).ToList();

            foreach (var stripe in _fileTail.Stripes)
            {
                var stripeStreams = stripe.GetStripeStreamCollection();

                var present = new ColumnReader(stripeStreams, columnId).ReadBooleanStream(StreamKind.Present);

                var readAndSetters = properties.Select(p => GetReadAndSetterForColumn(p.propertyInfo, stripeStreams, p.columnId, p.columnType)).ToList();

                for (ulong i = 0; i < stripe.NumRows; i++)
                {
                    if (present[i])
                    {
                        var obj = Activator.CreateInstance(type);
                        foreach (var readAndSetter in readAndSetters)
                        {
                            readAndSetter(obj);
                        }
                        yield return obj; 
                    }
                    else
                    {
                        yield return null;
                    }   
                }
            }
        }

        IEnumerable<(PropertyInfo propertyInfo, uint columnId, ColumnTypeKind columnType)> FindColumnsForType(Type type, 
            Footer footer, int parentColumnId = 0)
        {
            foreach (var property in GetWritablePublicProperties(type))
            {
                var columnIdIndex = footer.Types[parentColumnId].FieldNames
                    .FindIndex(fn => fn.ToLower().TrimStart('_') == property.Name.ToLower());
                if (columnIdIndex < 0)
                {
                    if (_ignoreMissingColumns)
                        continue;
                    else
                        throw new KeyNotFoundException($"'{property.Name}' not found in ORC data");
                }
                var columnId = footer.Types[parentColumnId].SubTypes[columnIdIndex];
                var columnType = footer.Types[(int)columnId].Kind;
                yield return (property, columnId, columnType);
            }
        }

        static IEnumerable<PropertyInfo> GetWritablePublicProperties(Type type)
        {
            return type.GetTypeInfo().DeclaredProperties.Where(p => p.SetMethod != null);
        }

        Action<object> GetReadAndSetterForColumn(PropertyInfo propertyInfo, StripeStreamReaderCollection stripeStreams, uint columnId, ColumnTypeKind columnType)
        {
            switch (columnType)
            {
                case ColumnTypeKind.Long:
                case ColumnTypeKind.Int:
                case ColumnTypeKind.Short:
                    return GetValueSetterEnumerable(propertyInfo, new LongReader(stripeStreams, columnId).Read());
                case ColumnTypeKind.Byte:
                    return GetValueSetterEnumerable(propertyInfo, new ByteReader(stripeStreams, columnId).Read());
                case ColumnTypeKind.Boolean:
                    return GetValueSetterEnumerable(propertyInfo, new BooleanReader(stripeStreams, columnId).Read());
                case ColumnTypeKind.Float:
                    return GetValueSetterEnumerable(propertyInfo, new FloatReader(stripeStreams, columnId).Read());
                case ColumnTypeKind.Double:
                    return GetValueSetterEnumerable(propertyInfo, new DoubleReader(stripeStreams, columnId).Read());
                case ColumnTypeKind.Binary:
                    return GetValueSetterEnumerable(propertyInfo, new ColumnTypes.BinaryReader(stripeStreams, columnId).Read());
                case ColumnTypeKind.Decimal:
                    return GetValueSetterEnumerable(propertyInfo, new DecimalReader(stripeStreams, columnId).Read());
                case ColumnTypeKind.Timestamp:
                    return GetValueSetterEnumerable(propertyInfo, new TimestampReader(stripeStreams, columnId).Read());
                case ColumnTypeKind.Date:
                    return GetValueSetterEnumerable(propertyInfo, new DateReader(stripeStreams, columnId).Read());
                case ColumnTypeKind.String:
                    return GetValueSetterEnumerable(propertyInfo, new ColumnTypes.StringReader(stripeStreams, columnId).Read());
                case ColumnTypeKind.Struct:
                    Type type = propertyInfo.PropertyType;
                    if (type.IsClass && !type.FullName.StartsWith("System."))
                    {
                        return GetValueSetterEnumerable(propertyInfo, ReadSubType(type, columnId));
                    }
                    else
                    {
                        return GetValueSetterEnumerable(propertyInfo, new StructReader(stripeStreams, columnId
                                    , _fileTail.Footer.Types[(int)columnId].SubTypes.ToArray()).Read(type));
                    }
                default:
                    throw new NotImplementedException($"Column type {columnType} is not supported");
            }
        }

        static Action<object> GetValueSetterEnumerable<T>(PropertyInfo propertyInfo, IEnumerable<T> enumerable)
        {
            var valueSetter = GetValueSetter<T>(propertyInfo);
            var enumerator = enumerable.GetEnumerator();
            return instance =>
            {
                if (!enumerator.MoveNext())
                    throw new InvalidOperationException("Read past the end of data");
                valueSetter(instance, enumerator.Current);
            };
        }

        static Action<object, FromT> GetValueSetter<FromT>(PropertyInfo propertyInfo)
        {
            var instance = Expression.Parameter(typeof(object), "instance");
            var value = Expression.Parameter(typeof(FromT), "value");
            var valueAsType = Expression.Convert(value, propertyInfo.PropertyType);
            var instanceAsType = Expression.Convert(instance, propertyInfo.DeclaringType);
            var callSetter = Expression.Call(instanceAsType, propertyInfo.GetSetMethod(), valueAsType);
            var parameters = new ParameterExpression[] { instance, value };

            return Expression.Lambda<Action<object, FromT>>(callSetter, parameters).Compile();
        }
    }
}
