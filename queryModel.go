package GoRedisDao

// TODO: use operator to replace the mess of query model field
type DaoQueryOperator int

const (
	DaoQueryOperator_NONE DaoQueryOperator = iota
	DaoQueryOperator_EQ
	DaoQueryOperator_NEQ
	DaoQueryOperator_LT
	DaoQueryOperator_LTE
	DaoQueryOperator_GT
	DaoQueryOperator_GTE
	DaoQueryOperator_IN
	DaoQueryOperator_NIN
	DaoQueryOperator_OTHER1 // define your own operator in your code space
	DaoQueryOperator_OTHER2 // define your own operator in your code space
	DaoQueryOperator_OTHER3 // define your own operator in your code space
)

/* the other query options to do in the future
delete     bool
update     map[string]interface{}
withs      []argClause
selectCols []string
count      bool
from       []string
joins      []join
where      []where
groupBy    []string
orderBy    []argClause
having     []argClause
limit      int
offset     int
forlock    string
distinct   string
comment    string
*/
