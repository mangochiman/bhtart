{
	"language": "javascript",
  "views": {
    "by_rule_id": {
      "map": "function(doc) { emit(doc['rule_id']) }"
    },
		"by_date_checked": {
      "map": "function(doc) { emit(doc['date_checked']) }"
    },
		"by_rule_id_and_date_checked": {
      "map": "function(doc) { emit([doc['rule_id'], doc['date_checked']]) }"
    }
  }
}
