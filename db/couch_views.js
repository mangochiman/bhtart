{
  "views": {  
   "by_rule_and_date_checked": {
      "map": "function(doc) { emit(doc['rule'].trim().split(' ').join('_') + '_' + doc['date_checked'].substr(0, 8)) }"
    }
  }
}
