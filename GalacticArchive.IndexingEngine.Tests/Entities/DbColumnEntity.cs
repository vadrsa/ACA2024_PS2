using System.Data;
using static System.Int32;

namespace GalacticArchive.IndexingEngine.Tests.Entities;

public class DbColumnEntity
{
    private DbColumnEntity(string name, string dataType, int maxLength, bool isNullable)
    {
        Name = name;
        DataType = dataType;
        MaxLength = maxLength;
        IsNullable = isNullable;
    }

    public string Name { get; }
    public string DataType { get; }
    public int MaxLength { get; }
    public bool IsNullable { get; }

    public static DbColumnEntity FromDataRow(DataRow dataRow)
    {
        TryParse(dataRow["CHARACTER_MAXIMUM_LENGTH"].ToString()!, out var maxLength);
        return new(name: dataRow["COLUMN_NAME"].ToString()!, dataType: dataRow["DATA_TYPE"].ToString()!,
            maxLength: maxLength,
            isNullable: !dataRow["IS_NULLABLE"].Equals("NO"));
    }
}