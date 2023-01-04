using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Stripes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class StructReader : ColumnReader
    {
        readonly uint[] _subColumnIds;

        public StructReader(StripeStreamReaderCollection stripeStreams, uint columnId, uint[] subColumnIds) : base(stripeStreams, columnId)
        {
            _subColumnIds = subColumnIds;
        }

        public IEnumerable<object> Read(Type propertyType)
        {
            var kind = _stripeStreams.FirstOrDefault(s => s.ColumnId == _subColumnIds[0] && s.StreamKind == Protocol.StreamKind.Data);


            if (kind?.ColumnEncodingKind == Protocol.ColumnEncodingKind.DirectV2)
            {
                var present = ReadBooleanStream(Protocol.StreamKind.Present);
                var stripeStreamData = _stripeStreams.FirstOrDefault(s => s.ColumnId == _subColumnIds[0] && s.StreamKind == Protocol.StreamKind.Data);
                var stripeStreamLength = _stripeStreams.FirstOrDefault(s => s.ColumnId == _subColumnIds[0] && s.StreamKind == Protocol.StreamKind.Length);
                if (stripeStreamData == null || stripeStreamLength == null)
                    throw new InvalidDataException("DATA or LENGTH streams must be available");


                var stream = stripeStreamData.GetDecompressedStream();
                var memStream = new MemoryStream();
                stream.CopyTo(memStream);
                var data = memStream.ToArray();

                var streamLength = stripeStreamLength.GetDecompressedStream();
                var reader = new IntegerRunLengthEncodingV2Reader(streamLength, false);
                var length = reader.Read().ToArray();

                int stringOffset = 0;
                if (present == null)
                {
                    foreach (var len in length)
                    {
                        var value = Encoding.UTF8.GetString(data, stringOffset, (int)len);
                        stringOffset += (int)len;
                        yield return CalculateValue(propertyType, value);
                    }
                }
                else
                {
                    var lengthEnumerator = ((IEnumerable<long>)length).GetEnumerator();
                    foreach (var isPresent in present)
                    {
                        if (isPresent)
                        {
                            var success = lengthEnumerator.MoveNext();
                            if (!success)
                                throw new InvalidDataException("The PRESENT data stream's length didn't match the LENGTH stream's length");
                            var len = lengthEnumerator.Current;
                            var value = Encoding.UTF8.GetString(data, stringOffset, (int)len);
                            stringOffset += (int)len;
                            yield return CalculateValue(propertyType, value);
                        }
                        else
                            yield return null;
                    }
                }

                yield return null;
            }
            else
            {
                var present = ReadBooleanStream(Protocol.StreamKind.Present);

                var stripeStreamData = _stripeStreams.FirstOrDefault(s => s.ColumnId == _subColumnIds[0] && s.StreamKind == Protocol.StreamKind.Data);
                var stripeStreamLength = _stripeStreams.FirstOrDefault(s => s.ColumnId == _subColumnIds[0] && s.StreamKind == Protocol.StreamKind.Length);
                var stripeStreamDictionary = _stripeStreams.FirstOrDefault(s => s.ColumnId == _subColumnIds[0] && s.StreamKind == Protocol.StreamKind.DictionaryData);

                var stream = stripeStreamData.GetDecompressedStream();
                var reader = new IntegerRunLengthEncodingV2Reader(stream, false);
                var data = reader.Read().ToArray();

                var streamDictionary = stripeStreamDictionary.GetDecompressedStream();
                var memStream = new MemoryStream();
                streamDictionary.CopyTo(memStream);
                var dictionaryData = memStream.ToArray();

                var streamLength = stripeStreamLength.GetDecompressedStream();
                var readerLength = new IntegerRunLengthEncodingV2Reader(streamLength, false);
                var length = readerLength.Read().ToArray();

                if (data == null || dictionaryData == null || length == null)
                    throw new InvalidDataException("DATA, DICTIONARY_DATA, and LENGTH streams must be available");

                var dictionary = new List<string>();
                int stringOffset = 0;
                foreach (var len in length)
                {
                    var dictionaryValue = Encoding.UTF8.GetString(dictionaryData, stringOffset, (int)len);
                    stringOffset += (int)len;
                    dictionary.Add(dictionaryValue);
                }

                if (present == null)
                {
                    foreach (var value in data)
                        yield return CalculateValue(propertyType, dictionary[(int)value]);
                }
                else
                {
                    var valueEnumerator = ((IEnumerable<long>)data).GetEnumerator();
                    foreach (var isPresent in present)
                    {
                        if (isPresent)
                        {
                            var success = valueEnumerator.MoveNext();
                            if (!success)
                                throw new InvalidDataException("The PRESENT data stream's length didn't match the DATA stream's length");
                            yield return CalculateValue(propertyType, dictionary[(int)valueEnumerator.Current]);
                        }
                        else
                            yield return null;
                    }
                }
            }
        }

        IEnumerable<object> ReadDictionaryV2(Type propertyType)
        {
            var present = ReadBooleanStream(Protocol.StreamKind.Present);

            var stripeStreamData = _stripeStreams.FirstOrDefault(s => s.ColumnId == _subColumnIds[0] && s.StreamKind == Protocol.StreamKind.Data);
            var stripeStreamLength = _stripeStreams.FirstOrDefault(s => s.ColumnId == _subColumnIds[0] && s.StreamKind == Protocol.StreamKind.Length);
            var stripeStreamDictionary = _stripeStreams.FirstOrDefault(s => s.ColumnId == _subColumnIds[0] && s.StreamKind == Protocol.StreamKind.DictionaryData);

            var stream = stripeStreamData.GetDecompressedStream();
            var reader = new IntegerRunLengthEncodingV2Reader(stream, false);
            var data = reader.Read().ToArray();

            var streamDictionary = stripeStreamDictionary.GetDecompressedStream();
            var memStream = new MemoryStream();
            streamDictionary.CopyTo(memStream);
            var dictionaryData = memStream.ToArray();

            var streamLength = stripeStreamLength.GetDecompressedStream();
            var readerLength = new IntegerRunLengthEncodingV2Reader(streamLength, false);
            var length = readerLength.Read().ToArray();

            if (data == null || dictionaryData == null || length == null)
                throw new InvalidDataException("DATA, DICTIONARY_DATA, and LENGTH streams must be available");

            var dictionary = new List<string>();
            int stringOffset = 0;
            foreach (var len in length)
            {
                var dictionaryValue = Encoding.UTF8.GetString(dictionaryData, stringOffset, (int)len);
                stringOffset += (int)len;
                dictionary.Add(dictionaryValue);
            }

            if (present == null)
            {
                foreach (var value in data)
                    yield return CalculateValue(propertyType, dictionary[(int)value]);
            }
            else
            {
                var valueEnumerator = ((IEnumerable<long>)data).GetEnumerator();
                foreach (var isPresent in present)
                {
                    if (isPresent)
                    {
                        var success = valueEnumerator.MoveNext();
                        if (!success)
                            throw new InvalidDataException("The PRESENT data stream's length didn't match the DATA stream's length");
                        yield return CalculateValue(propertyType, dictionary[(int)valueEnumerator.Current]);
                    }
                    else
                        yield return null;
                }
            }
        }

        private object CalculateValue(Type propertyType, string value)
        {
            if (value != null)
            {
                if (propertyType == typeof(decimal) || propertyType == typeof(decimal?))
                {
                    return decimal.TryParse(value, out var valueAsDecimal) ? valueAsDecimal : null;
                }
                else if (propertyType == typeof(DateTimeOffset) || propertyType == typeof(DateTimeOffset?))
                {
                    return DateTimeOffset.TryParse(value, out var valueAsDateTime) ? valueAsDateTime : null;
                }
                else
                {
                    return value;
                }
            }

            return null;
        }

    }
}
