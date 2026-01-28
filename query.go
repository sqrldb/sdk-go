package squirreldb

import (
	"encoding/json"
	"fmt"
	"strings"
)

// FilterOp represents a filter operation
type FilterOp interface {
	compile(field string) string
}

type eqOp struct{ value interface{} }
type neOp struct{ value interface{} }
type gtOp struct{ value float64 }
type gteOp struct{ value float64 }
type ltOp struct{ value float64 }
type lteOp struct{ value float64 }
type inOp struct{ values []interface{} }
type notInOp struct{ values []interface{} }
type containsOp struct{ value string }
type startsWithOp struct{ value string }
type endsWithOp struct{ value string }
type existsOp struct{ value bool }

func (o eqOp) compile(field string) string {
	v, _ := json.Marshal(o.value)
	return fmt.Sprintf("doc.%s === %s", field, v)
}

func (o neOp) compile(field string) string {
	v, _ := json.Marshal(o.value)
	return fmt.Sprintf("doc.%s !== %s", field, v)
}

func (o gtOp) compile(field string) string {
	return fmt.Sprintf("doc.%s > %v", field, o.value)
}

func (o gteOp) compile(field string) string {
	return fmt.Sprintf("doc.%s >= %v", field, o.value)
}

func (o ltOp) compile(field string) string {
	return fmt.Sprintf("doc.%s < %v", field, o.value)
}

func (o lteOp) compile(field string) string {
	return fmt.Sprintf("doc.%s <= %v", field, o.value)
}

func (o inOp) compile(field string) string {
	v, _ := json.Marshal(o.values)
	return fmt.Sprintf("%s.includes(doc.%s)", v, field)
}

func (o notInOp) compile(field string) string {
	v, _ := json.Marshal(o.values)
	return fmt.Sprintf("!%s.includes(doc.%s)", v, field)
}

func (o containsOp) compile(field string) string {
	v, _ := json.Marshal(o.value)
	return fmt.Sprintf("doc.%s.includes(%s)", field, v)
}

func (o startsWithOp) compile(field string) string {
	v, _ := json.Marshal(o.value)
	return fmt.Sprintf("doc.%s.startsWith(%s)", field, v)
}

func (o endsWithOp) compile(field string) string {
	v, _ := json.Marshal(o.value)
	return fmt.Sprintf("doc.%s.endsWith(%s)", field, v)
}

func (o existsOp) compile(field string) string {
	if o.value {
		return fmt.Sprintf("doc.%s !== undefined", field)
	}
	return fmt.Sprintf("doc.%s === undefined", field)
}

// FilterCondition represents a filter condition
type FilterCondition map[string]interface{}

// Field creates a field expression for building filters
type Field string

// Eq creates an equality filter
func (f Field) Eq(value interface{}) FilterCondition {
	return FilterCondition{string(f): eqOp{value}}
}

// Ne creates a not-equal filter
func (f Field) Ne(value interface{}) FilterCondition {
	return FilterCondition{string(f): neOp{value}}
}

// Gt creates a greater-than filter
func (f Field) Gt(value float64) FilterCondition {
	return FilterCondition{string(f): gtOp{value}}
}

// Gte creates a greater-than-or-equal filter
func (f Field) Gte(value float64) FilterCondition {
	return FilterCondition{string(f): gteOp{value}}
}

// Lt creates a less-than filter
func (f Field) Lt(value float64) FilterCondition {
	return FilterCondition{string(f): ltOp{value}}
}

// Lte creates a less-than-or-equal filter
func (f Field) Lte(value float64) FilterCondition {
	return FilterCondition{string(f): lteOp{value}}
}

// In creates an in-list filter
func (f Field) In(values ...interface{}) FilterCondition {
	return FilterCondition{string(f): inOp{values}}
}

// NotIn creates a not-in-list filter
func (f Field) NotIn(values ...interface{}) FilterCondition {
	return FilterCondition{string(f): notInOp{values}}
}

// Contains creates a string contains filter
func (f Field) Contains(value string) FilterCondition {
	return FilterCondition{string(f): containsOp{value}}
}

// StartsWith creates a string starts-with filter
func (f Field) StartsWith(value string) FilterCondition {
	return FilterCondition{string(f): startsWithOp{value}}
}

// EndsWith creates a string ends-with filter
func (f Field) EndsWith(value string) FilterCondition {
	return FilterCondition{string(f): endsWithOp{value}}
}

// Exists creates an existence filter
func (f Field) Exists(value bool) FilterCondition {
	return FilterCondition{string(f): existsOp{value}}
}

// And combines conditions with AND
func And(conditions ...FilterCondition) FilterCondition {
	return FilterCondition{"$and": conditions}
}

// Or combines conditions with OR
func Or(conditions ...FilterCondition) FilterCondition {
	return FilterCondition{"$or": conditions}
}

// Not negates a condition
func Not(condition FilterCondition) FilterCondition {
	return FilterCondition{"$not": condition}
}

func compileFilter(condition FilterCondition) string {
	var parts []string

	for field, value := range condition {
		switch field {
		case "$and":
			if conds, ok := value.([]FilterCondition); ok {
				var subParts []string
				for _, c := range conds {
					subParts = append(subParts, compileFilter(c))
				}
				parts = append(parts, fmt.Sprintf("(%s)", strings.Join(subParts, " && ")))
			}
		case "$or":
			if conds, ok := value.([]FilterCondition); ok {
				var subParts []string
				for _, c := range conds {
					subParts = append(subParts, compileFilter(c))
				}
				parts = append(parts, fmt.Sprintf("(%s)", strings.Join(subParts, " || ")))
			}
		case "$not":
			if cond, ok := value.(FilterCondition); ok {
				parts = append(parts, fmt.Sprintf("!(%s)", compileFilter(cond)))
			}
		default:
			if op, ok := value.(FilterOp); ok {
				parts = append(parts, op.compile(field))
			} else {
				// Direct equality
				v, _ := json.Marshal(value)
				parts = append(parts, fmt.Sprintf("doc.%s === %s", field, v))
			}
		}
	}

	if len(parts) == 0 {
		return "true"
	}
	return strings.Join(parts, " && ")
}

// SortDirection represents sort direction
type SortDirection string

const (
	Asc  SortDirection = "asc"
	Desc SortDirection = "desc"
)

// SortSpec represents a sort specification
type SortSpec struct {
	Field     string
	Direction SortDirection
}

// QueryBuilder builds queries for SquirrelDB
// Uses MongoDB-like naming: Find/Sort/Limit
type QueryBuilder struct {
	tableName  string
	filterExpr string
	sortSpecs  []SortSpec
	limitValue *int
	skipValue  *int
	isChanges  bool
}

// Table creates a new query builder for a table
func Table(name string) *QueryBuilder {
	return &QueryBuilder{tableName: name}
}

// Find adds a filter condition
// Usage: Table("users").Find(Field("age").Gt(21))
func (q *QueryBuilder) Find(condition FilterCondition) *QueryBuilder {
	q.filterExpr = compileFilter(condition)
	return q
}

// Sort adds a sort specification
// Usage: .Sort("name", Asc) or .Sort("age", Desc)
func (q *QueryBuilder) Sort(field string, direction SortDirection) *QueryBuilder {
	q.sortSpecs = append(q.sortSpecs, SortSpec{Field: field, Direction: direction})
	return q
}

// Limit limits the number of results
func (q *QueryBuilder) Limit(n int) *QueryBuilder {
	q.limitValue = &n
	return q
}

// Skip skips results (offset)
func (q *QueryBuilder) Skip(n int) *QueryBuilder {
	q.skipValue = &n
	return q
}

// Changes sets the query to subscribe to changes
func (q *QueryBuilder) Changes() *QueryBuilder {
	q.isChanges = true
	return q
}

// Compile compiles the query to SquirrelDB JS syntax
func (q *QueryBuilder) Compile() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(`db.table("%s")`, q.tableName))

	if q.filterExpr != "" {
		sb.WriteString(fmt.Sprintf(".filter(doc => %s)", q.filterExpr))
	}

	for _, spec := range q.sortSpecs {
		if spec.Direction == Desc {
			sb.WriteString(fmt.Sprintf(`.orderBy("%s", "desc")`, spec.Field))
		} else {
			sb.WriteString(fmt.Sprintf(`.orderBy("%s")`, spec.Field))
		}
	}

	if q.limitValue != nil {
		sb.WriteString(fmt.Sprintf(".limit(%d)", *q.limitValue))
	}

	if q.skipValue != nil {
		sb.WriteString(fmt.Sprintf(".skip(%d)", *q.skipValue))
	}

	if q.isChanges {
		sb.WriteString(".changes()")
	} else {
		sb.WriteString(".run()")
	}

	return sb.String()
}

// String returns the compiled query
func (q *QueryBuilder) String() string {
	return q.Compile()
}
