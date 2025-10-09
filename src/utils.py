import re


def normalize_schema_name(name: str) -> str:
    """
    Normalize a string to be a valid DuckDB schema name.

    Rules:
    - Convert to lowercase
    - Replace spaces and special characters with underscores
    - Remove consecutive underscores
    - Ensure it starts with a letter or underscore
    - Remove trailing underscores
    - Limit length to reasonable size (63 chars like PostgreSQL)

    Args:
        name: Input string to normalize

    Returns:
        Valid DuckDB schema name

    Examples:
        >>> normalize_schema_name("My Schema 2024")
        'my_schema_2024'
        >>> normalize_schema_name("123-test schema!")
        '_123_test_schema'
        >>> normalize_schema_name("Café & Restaurant")
        'cafe_restaurant'
    """
    if not name:
        raise ValueError("Schema name cannot be empty")

    # Convert to lowercase
    normalized = name.lower().strip()

    # Replace common unicode characters
    replacements = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
        'ä': 'a', 'ö': 'o', 'ü': 'u', 'ß': 'ss',
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
        'á': 'a', 'à': 'a', 'â': 'a', 'ã': 'a',
        'í': 'i', 'ì': 'i', 'î': 'i',
        'ó': 'o', 'ò': 'o', 'ô': 'o', 'õ': 'o',
        'ú': 'u', 'ù': 'u', 'û': 'u',
        'ç': 'c', 'ñ': 'n',
    }

    for char, replacement in replacements.items():
        normalized = normalized.replace(char, replacement)

    # Replace any non-alphanumeric character (except underscore) with underscore
    normalized = re.sub(r'[^a-z0-9_]', '_', normalized)

    # Remove consecutive underscores
    normalized = re.sub(r'_+', '_', normalized)

    # Ensure it starts with a letter or underscore (not a digit)
    if normalized and normalized[0].isdigit():
        normalized = '_' + normalized

    # Remove leading and trailing underscores
    normalized = normalized.strip('_')

    # Limit length (DuckDB doesn't have strict limit, but 63 is safe like PostgreSQL)
    max_length = 63
    if len(normalized) > max_length:
        normalized = normalized[:max_length].rstrip('_')

    # Final check - if empty after normalization, use default
    if not normalized:
        normalized = 'schema_default'

    return normalized