// SquirrelDB Go SDK - Query Builder Tests

package squirreldb

import (
	"encoding/json"
	"testing"
)

func TestFieldExprEq(t *testing.T) {
	cond := Field("age").Eq(25)
	if cond.Field != "age" {
		t.Errorf("Expected field 'age', got '%s'", cond.Field)
	}
	if cond.Operator != "$eq" {
		t.Errorf("Expected operator '$eq', got '%s'", cond.Operator)
	}
	if cond.Value != 25 {
		t.Errorf("Expected value 25, got '%v'", cond.Value)
	}
}

func TestFieldExprNe(t *testing.T) {
	cond := Field("status").Ne("inactive")
	if cond.Operator != "$ne" {
		t.Errorf("Expected operator '$ne', got '%s'", cond.Operator)
	}
	if cond.Value != "inactive" {
		t.Errorf("Expected value 'inactive', got '%v'", cond.Value)
	}
}

func TestFieldExprGt(t *testing.T) {
	cond := Field("price").Gt(100)
	if cond.Operator != "$gt" {
		t.Errorf("Expected operator '$gt', got '%s'", cond.Operator)
	}
}

func TestFieldExprGte(t *testing.T) {
	cond := Field("count").Gte(10)
	if cond.Operator != "$gte" {
		t.Errorf("Expected operator '$gte', got '%s'", cond.Operator)
	}
}

func TestFieldExprLt(t *testing.T) {
	cond := Field("age").Lt(18)
	if cond.Operator != "$lt" {
		t.Errorf("Expected operator '$lt', got '%s'", cond.Operator)
	}
}

func TestFieldExprLte(t *testing.T) {
	cond := Field("rating").Lte(5)
	if cond.Operator != "$lte" {
		t.Errorf("Expected operator '$lte', got '%s'", cond.Operator)
	}
}

func TestFieldExprIn(t *testing.T) {
	cond := Field("role").In("admin", "mod")
	if cond.Operator != "$in" {
		t.Errorf("Expected operator '$in', got '%s'", cond.Operator)
	}
	values := cond.Value.([]interface{})
	if len(values) != 2 {
		t.Errorf("Expected 2 values, got %d", len(values))
	}
}

func TestFieldExprNotIn(t *testing.T) {
	cond := Field("status").NotIn("banned", "deleted")
	if cond.Operator != "$nin" {
		t.Errorf("Expected operator '$nin', got '%s'", cond.Operator)
	}
}

func TestFieldExprContains(t *testing.T) {
	cond := Field("name").Contains("test")
	if cond.Operator != "$contains" {
		t.Errorf("Expected operator '$contains', got '%s'", cond.Operator)
	}
}

func TestFieldExprStartsWith(t *testing.T) {
	cond := Field("email").StartsWith("admin")
	if cond.Operator != "$startsWith" {
		t.Errorf("Expected operator '$startsWith', got '%s'", cond.Operator)
	}
}

func TestFieldExprEndsWith(t *testing.T) {
	cond := Field("email").EndsWith(".com")
	if cond.Operator != "$endsWith" {
		t.Errorf("Expected operator '$endsWith', got '%s'", cond.Operator)
	}
}

func TestFieldExprExists(t *testing.T) {
	cond := Field("avatar").Exists(true)
	if cond.Operator != "$exists" {
		t.Errorf("Expected operator '$exists', got '%s'", cond.Operator)
	}
	if cond.Value != true {
		t.Errorf("Expected value true, got '%v'", cond.Value)
	}
}

func TestFieldExprExistsFalse(t *testing.T) {
	cond := Field("deleted_at").Exists(false)
	if cond.Value != false {
		t.Errorf("Expected value false, got '%v'", cond.Value)
	}
}

func TestTableCreatesQueryBuilder(t *testing.T) {
	query := Table("users")
	if query == nil {
		t.Error("Expected non-nil query builder")
	}
}

func TestCompileMinimalQuery(t *testing.T) {
	result := Table("users").CompileStructured()
	if result.Table != "users" {
		t.Errorf("Expected table 'users', got '%s'", result.Table)
	}
	if result.Filter != nil {
		t.Error("Expected filter to be nil")
	}
}

func TestFindAddsFilter(t *testing.T) {
	result := Table("users").Find(Field("age").Gt(21)).CompileStructured()

	if result.Table != "users" {
		t.Errorf("Expected table 'users', got '%s'", result.Table)
	}
	if result.Filter == nil {
		t.Fatal("Expected filter to be non-nil")
	}
	if result.Filter["age"]["$gt"] != 21 {
		t.Errorf("Expected filter age.$gt = 21, got %v", result.Filter["age"]["$gt"])
	}
}

func TestMultipleFilters(t *testing.T) {
	result := Table("users").
		Find(Field("age").Gte(18)).
		Find(Field("age").Lte(65)).
		CompileStructured()

	if result.Filter["age"]["$gte"] != 18 {
		t.Errorf("Expected filter age.$gte = 18")
	}
	if result.Filter["age"]["$lte"] != 65 {
		t.Errorf("Expected filter age.$lte = 65")
	}
}

func TestSortAddsSortSpecification(t *testing.T) {
	result := Table("users").Sort("name", SortAsc).CompileStructured()

	if len(result.Sort) != 1 {
		t.Fatalf("Expected 1 sort, got %d", len(result.Sort))
	}
	if result.Sort[0].Field != "name" {
		t.Errorf("Expected sort field 'name', got '%s'", result.Sort[0].Field)
	}
	if result.Sort[0].Direction != SortAsc {
		t.Errorf("Expected sort direction 'asc', got '%s'", result.Sort[0].Direction)
	}
}

func TestSortDesc(t *testing.T) {
	result := Table("users").Sort("created_at", SortDesc).CompileStructured()

	if result.Sort[0].Direction != SortDesc {
		t.Errorf("Expected sort direction 'desc', got '%s'", result.Sort[0].Direction)
	}
}

func TestMultipleSorts(t *testing.T) {
	result := Table("posts").
		Sort("pinned", SortDesc).
		Sort("created_at", SortDesc).
		CompileStructured()

	if len(result.Sort) != 2 {
		t.Fatalf("Expected 2 sorts, got %d", len(result.Sort))
	}
	if result.Sort[0].Field != "pinned" {
		t.Errorf("Expected first sort field 'pinned', got '%s'", result.Sort[0].Field)
	}
	if result.Sort[1].Field != "created_at" {
		t.Errorf("Expected second sort field 'created_at', got '%s'", result.Sort[1].Field)
	}
}

func TestLimitSetsMaxResults(t *testing.T) {
	result := Table("users").Limit(10).CompileStructured()

	if result.Limit == nil {
		t.Fatal("Expected limit to be non-nil")
	}
	if *result.Limit != 10 {
		t.Errorf("Expected limit 10, got %d", *result.Limit)
	}
}

func TestSkipSetsOffset(t *testing.T) {
	result := Table("users").Skip(20).CompileStructured()

	if result.Skip == nil {
		t.Fatal("Expected skip to be non-nil")
	}
	if *result.Skip != 20 {
		t.Errorf("Expected skip 20, got %d", *result.Skip)
	}
}

func TestChangesEnablesSubscription(t *testing.T) {
	result := Table("messages").Changes(nil).CompileStructured()

	if result.Changes == nil {
		t.Fatal("Expected changes to be non-nil")
	}
	if result.Changes.IncludeInitial != true {
		t.Error("Expected includeInitial to be true")
	}
}

func TestChangesWithOptions(t *testing.T) {
	result := Table("messages").Changes(&ChangesOptions{IncludeInitial: false}).CompileStructured()

	if result.Changes.IncludeInitial != false {
		t.Error("Expected includeInitial to be false")
	}
}

func TestFullQuery(t *testing.T) {
	result := Table("users").
		Find(Field("age").Gte(18)).
		Find(Field("status").Eq("active")).
		Sort("name", SortAsc).
		Limit(50).
		Skip(100).
		CompileStructured()

	if result.Table != "users" {
		t.Error("Expected table 'users'")
	}
	if result.Filter["age"]["$gte"] != 18 {
		t.Error("Expected filter age.$gte = 18")
	}
	if result.Filter["status"]["$eq"] != "active" {
		t.Error("Expected filter status.$eq = 'active'")
	}
	if len(result.Sort) != 1 {
		t.Error("Expected 1 sort")
	}
	if *result.Limit != 50 {
		t.Error("Expected limit 50")
	}
	if *result.Skip != 100 {
		t.Error("Expected skip 100")
	}
}

func TestCompileReturnsJSONString(t *testing.T) {
	result, err := Table("users").Limit(10).Compile()
	if err != nil {
		t.Fatalf("Failed to compile: %v", err)
	}

	var parsed map[string]interface{}
	err = json.Unmarshal([]byte(result), &parsed)
	if err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if parsed["table"] != "users" {
		t.Errorf("Expected table 'users', got '%v'", parsed["table"])
	}
}

func TestAndCombinesConditions(t *testing.T) {
	cond := And(
		Field("age").Gte(18),
		Field("active").Eq(true),
	)

	if cond.Field != "$and" {
		t.Errorf("Expected field '$and', got '%s'", cond.Field)
	}
	if cond.Operator != "$and" {
		t.Errorf("Expected operator '$and', got '%s'", cond.Operator)
	}
	conditions := cond.Value.([]FilterCondition)
	if len(conditions) != 2 {
		t.Errorf("Expected 2 conditions, got %d", len(conditions))
	}
}

func TestOrCombinesConditions(t *testing.T) {
	cond := Or(
		Field("role").Eq("admin"),
		Field("role").Eq("moderator"),
	)

	if cond.Field != "$or" {
		t.Errorf("Expected field '$or', got '%s'", cond.Field)
	}
}

func TestNotNegatesCondition(t *testing.T) {
	cond := Not(Field("banned").Eq(true))

	if cond.Field != "$not" {
		t.Errorf("Expected field '$not', got '%s'", cond.Field)
	}
}
