package server

import (
	"regexp"
	"strings"
)

type LineageResult struct {
	Tables  []string
	Fields  []string
	SQLType string
}

var (
	reFromTable   = regexp.MustCompile(`(?i)\bFROM\s+` + identPattern())
	reJoinTable   = regexp.MustCompile(`(?i)\bJOIN\s+` + identPattern())
	reIntoTable   = regexp.MustCompile(`(?i)\bINTO\s+` + identPattern())
	reUpdateTable = regexp.MustCompile(`(?i)\bUPDATE\s+` + identPattern())
	reDeleteFrom  = regexp.MustCompile(`(?i)\bDELETE\s+FROM\s+` + identPattern())
	reSelectCols  = regexp.MustCompile(`(?i)\bSELECT\s+(.+?)\bFROM\b`)
	reSetCols     = regexp.MustCompile(`(?i)\bSET\s+(.+?)(?:\bWHERE\b|$)`)
	reInsertCols  = regexp.MustCompile(`(?i)\bINSERT\s+(?:INTO\s+)?\w+(?:\.\w+)?\s*\((.+?)\)`)
	reWhereCols   = regexp.MustCompile(`(?i)\bWHERE\s+(.+?)$`)
	reColName     = regexp.MustCompile(`\b([a-zA-Z_][a-zA-Z0-9_]*)\b`)
	reBacktick    = regexp.MustCompile("`([^`]+)`")
)

func identPattern() string {
	return `(\w+(?:\.\w+)?)`
}

func ExtractLineage(sql string) *LineageResult {
	sql = reBacktick.ReplaceAllString(sql, "$1")
	result := &LineageResult{}

	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)

	switch {
	case strings.HasPrefix(upper, "SELECT"):
		result.SQLType = "SELECT"
		extractTables(sql, result, reFromTable, reJoinTable)
		extractSelectFields(sql, result)
	case strings.HasPrefix(upper, "INSERT"):
		result.SQLType = "INSERT"
		extractTables(sql, result, reIntoTable)
		extractInsertFields(sql, result)
	case strings.HasPrefix(upper, "UPDATE"):
		result.SQLType = "UPDATE"
		extractTables(sql, result, reUpdateTable, reJoinTable)
		extractSetFields(sql, result)
		extractWhereFields(sql, result)
	case strings.HasPrefix(upper, "DELETE"):
		result.SQLType = "DELETE"
		extractTables(sql, result, reDeleteFrom)
		extractWhereFields(sql, result)
	default:
		result.SQLType = "OTHER"
		extractTables(sql, result, reFromTable, reJoinTable, reIntoTable, reUpdateTable, reDeleteFrom)
	}

	dedupResult(result)
	return result
}

func extractTables(sql string, result *LineageResult, patterns ...*regexp.Regexp) {
	for _, re := range patterns {
		matches := re.FindAllStringSubmatch(sql, -1)
		for _, m := range matches {
			if len(m) > 1 {
				table := strings.ToLower(m[1])
				if isSQLKeyword(table) {
					continue
				}
				result.Tables = append(result.Tables, table)
			}
		}
	}
}

func extractSelectFields(sql string, result *LineageResult) {
	m := reSelectCols.FindStringSubmatch(sql)
	if m == nil {
		return
	}
	colsStr := m[1]
	for _, col := range strings.Split(colsStr, ",") {
		col = strings.TrimSpace(col)
		col = stripAlias(col)
		name := extractColName(col)
		if name != "" && !isSQLKeyword(name) {
			result.Fields = append(result.Fields, strings.ToLower(name))
		}
	}
}

func extractInsertFields(sql string, result *LineageResult) {
	m := reInsertCols.FindStringSubmatch(sql)
	if m == nil {
		return
	}
	for _, col := range strings.Split(m[1], ",") {
		name := extractColName(strings.TrimSpace(col))
		if name != "" && !isSQLKeyword(name) {
			result.Fields = append(result.Fields, strings.ToLower(name))
		}
	}
}

func extractSetFields(sql string, result *LineageResult) {
	m := reSetCols.FindStringSubmatch(sql)
	if m == nil {
		return
	}
	for _, assignment := range strings.Split(m[1], ",") {
		parts := strings.SplitN(assignment, "=", 2)
		name := extractColName(strings.TrimSpace(parts[0]))
		if name != "" && !isSQLKeyword(name) {
			result.Fields = append(result.Fields, strings.ToLower(name))
		}
	}
}

func extractWhereFields(sql string, result *LineageResult) {
	m := reWhereCols.FindStringSubmatch(sql)
	if m == nil {
		return
	}
	whereClause := m[1]
	tokens := reColName.FindAllString(whereClause, -1)
	for i, name := range tokens {
		lower := strings.ToLower(name)
		if isSQLKeyword(lower) {
			continue
		}
		if isLikelyValue(tokens, i) {
			continue
		}
		result.Fields = append(result.Fields, lower)
	}
}

func isLikelyValue(tokens []string, idx int) bool {
	if idx == 0 {
		return false
	}
	prev := tokens[idx-1]
	lower := strings.ToLower(prev)
	return lower == "=" || lower == ">" || lower == "<" || lower == "in" ||
		lower == "like" || lower == "between" || lower == "!="
}

func stripAlias(col string) string {
	lower := strings.ToLower(col)
	keywords := []string{" as ", " "}
	for _, kw := range keywords {
		if idx := strings.LastIndex(lower, kw); idx > 0 {
			col = col[:idx]
		}
	}
	return col
}

func extractColName(s string) string {
	s = strings.TrimSpace(s)
	if idx := strings.Index(s, "("); idx >= 0 {
		return ""
	}
	if idx := strings.Index(s, "."); idx >= 0 {
		s = s[idx+1:]
	}
	s = strings.TrimSpace(s)
	if s == "*" {
		return ""
	}
	return s
}

func isSQLKeyword(s string) bool {
	keywords := map[string]bool{
		"select": true, "from": true, "where": true, "and": true, "or": true,
		"not": true, "in": true, "is": true, "null": true, "like": true,
		"between": true, "exists": true, "join": true, "inner": true,
		"left": true, "right": true, "outer": true, "cross": true,
		"on": true, "as": true, "order": true, "by": true, "group": true,
		"having": true, "limit": true, "offset": true, "union": true,
		"all": true, "distinct": true, "insert": true, "into": true,
		"values": true, "update": true, "set": true, "delete": true,
		"create": true, "drop": true, "alter": true, "index": true,
		"table": true, "database": true, "schema": true, "view": true,
		"if": true, "else": true, "end": true, "case": true, "when": true,
		"then": true, "asc": true, "desc": true, "true": true, "false": true,
		"count": true, "sum": true, "avg": true, "min": true, "max": true,
		"now": true, "curdate": true, "curtime": true, "date": true,
		"int": true, "varchar": true, "text": true, "bigint": true,
		"float": true, "double": true, "decimal": true, "char": true,
		"primary": true, "key": true, "foreign": true, "references": true,
		"default": true, "constraint": true, "unique": true, "check": true,
	}
	return keywords[s]
}

func dedupResult(r *LineageResult) {
	r.Tables = dedup(r.Tables)
	r.Fields = dedup(r.Fields)
}

func dedup(ss []string) []string {
	seen := make(map[string]bool, len(ss))
	result := make([]string, 0, len(ss))
	for _, s := range ss {
		if !seen[s] {
			seen[s] = true
			result = append(result, s)
		}
	}
	return result
}
