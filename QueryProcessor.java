package db.query;

import db.core.Node;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryProcessor {
    private final Node node;
    private final QueryParser parser;
    private final QueryOptimizer optimizer;
    private final QueryExecutor executor;
    
    public QueryProcessor(Node node) {
        this.node = node;
        this.parser = new QueryParser();
        this.optimizer = new QueryOptimizer();
        this.executor = new QueryExecutor(node);
    }
    
    public CompletableFuture<String> process(String query) {
        try {
            ParsedQuery parsed = parser.parse(query);
            OptimizedQuery optimized = optimizer.optimize(parsed);
            return executor.execute(optimized);
        } catch (Exception e) {
            CompletableFuture<String> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }
}

class QueryParser {
    private static final Pattern SELECT_PATTERN = 
        Pattern.compile("SELECT\\s+(.+?)\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.+?))?(?:\\s+ORDER\\s+BY\\s+(.+?))?(?:\\s+LIMIT\\s+(\\d+))?", 
                       Pattern.CASE_INSENSITIVE);
    
    private static final Pattern INSERT_PATTERN = 
        Pattern.compile("INSERT\\s+INTO\\s+(\\w+)\\s*\\(([^)]+)\\)\\s*VALUES\\s*\\(([^)]+)\\)", 
                       Pattern.CASE_INSENSITIVE);
    
    private static final Pattern UPDATE_PATTERN = 
        Pattern.compile("UPDATE\\s+(\\w+)\\s+SET\\s+(.+?)(?:\\s+WHERE\\s+(.+?))?", 
                       Pattern.CASE_INSENSITIVE);
    
    private static final Pattern DELETE_PATTERN = 
        Pattern.compile("DELETE\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.+?))?", 
                       Pattern.CASE_INSENSITIVE);
    
    private static final Pattern BEGIN_PATTERN = 
        Pattern.compile("BEGIN\\s+TRANSACTION", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern COMMIT_PATTERN = 
        Pattern.compile("COMMIT", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern ROLLBACK_PATTERN = 
        Pattern.compile("ROLLBACK", Pattern.CASE_INSENSITIVE);
    
    public ParsedQuery parse(String query) throws QueryException {
        query = query.trim();
        
        if (BEGIN_PATTERN.matcher(query).matches()) {
            return new ParsedQuery(QueryType.BEGIN_TRANSACTION, null, null, null, null, null, null, 0);
        }
        
        if (COMMIT_PATTERN.matcher(query).matches()) {
            return new ParsedQuery(QueryType.COMMIT, null, null, null, null, null, null, 0);
        }
        
        if (ROLLBACK_PATTERN.matcher(query).matches()) {
            return new ParsedQuery(QueryType.ROLLBACK, null, null, null, null, null, null, 0);
        }
        
        Matcher selectMatcher = SELECT_PATTERN.matcher(query);
        if (selectMatcher.matches()) {
            return parseSelect(selectMatcher);
        }
        
        Matcher insertMatcher = INSERT_PATTERN.matcher(query);
        if (insertMatcher.matches()) {
            return parseInsert(insertMatcher);
        }
        
        Matcher updateMatcher = UPDATE_PATTERN.matcher(query);
        if (updateMatcher.matches()) {
            return parseUpdate(updateMatcher);
        }
        
        Matcher deleteMatcher = DELETE_PATTERN.matcher(query);
        if (deleteMatcher.matches()) {
            return parseDelete(deleteMatcher);
        }
        
        throw new QueryException("Unsupported query: " + query);
    }
    
    private ParsedQuery parseSelect(Matcher matcher) throws QueryException {
        String columns = matcher.group(1).trim();
        String table = matcher.group(2).trim();
        String whereClause = matcher.group(3);
        String orderBy = matcher.group(4);
        String limitStr = matcher.group(5);
        
        List<String> columnList = parseColumns(columns);
        WhereCondition where = whereClause != null ? parseWhere(whereClause) : null;
        List<OrderByClause> orderClauses = orderBy != null ? parseOrderBy(orderBy) : null;
        int limit = limitStr != null ? Integer.parseInt(limitStr) : 0;
        
        return new ParsedQuery(QueryType.SELECT, table, columnList, where, null, orderClauses, null, limit);
    }
    
    private ParsedQuery parseInsert(Matcher matcher) throws QueryException {
        String table = matcher.group(1).trim();
        String columns = matcher.group(2).trim();
        String values = matcher.group(3).trim();
        
        List<String> columnList = Arrays.asList(columns.split(","));
        for (int i = 0; i < columnList.size(); i++) {
            columnList.set(i, columnList.get(i).trim());
        }
        
        List<String> valueList = Arrays.asList(values.split(","));
        for (int i = 0; i < valueList.size(); i++) {
            valueList.set(i, valueList.get(i).trim().replaceAll("^['\"]|['\"]$", ""));
        }
        
        Map<String, String> assignments = new HashMap<>();
        for (int i = 0; i < columnList.size(); i++) {
            assignments.put(columnList.get(i), valueList.get(i));
        }
        
        return new ParsedQuery(QueryType.INSERT, table, columnList, null, assignments, null, valueList, 0);
    }
    
    private ParsedQuery parseUpdate(Matcher matcher) throws QueryException {
        String table = matcher.group(1).trim();
        String setClause = matcher.group(2).trim();
        String whereClause = matcher.group(3);
        
        Map<String, String> assignments = parseAssignments(setClause);
        WhereCondition where = whereClause != null ? parseWhere(whereClause) : null;
        
        return new ParsedQuery(QueryType.UPDATE, table, null, where, assignments, null, null, 0);
    }
    
    private ParsedQuery parseDelete(Matcher matcher) throws QueryException {
        String table = matcher.group(1).trim();
        String whereClause = matcher.group(2);
        
        WhereCondition where = whereClause != null ? parseWhere(whereClause) : null;
        
        return new ParsedQuery(QueryType.DELETE, table, null, where, null, null, null, 0);
    }
    
    private List<String> parseColumns(String columns) {
        if ("*".equals(columns)) {
            return Arrays.asList("*");
        }
        
        List<String> result = new ArrayList<>();
        for (String col : columns.split(",")) {
            result.add(col.trim());
        }
        return result;
    }
    
    private WhereCondition parseWhere(String whereClause) throws QueryException {
        String[] parts = whereClause.split("\\s*(=|!=|<|>|<=|>=)\\s*", 2);
        if (parts.length != 2) {
            throw new QueryException("Invalid WHERE clause: " + whereClause);
        }
        
        String column = parts[0].trim();
        String value = parts[1].trim().replaceAll("^['\"]|['\"]$", "");
        
        String operator = whereClause.replaceAll(".*\\s(" + parts[0].trim() + ")\\s*", "")
                                   .replaceAll("\\s*" + Pattern.quote(parts[1].trim()) + ".*", "")
                                   .trim();
        
        return new WhereCondition(column, operator, value);
    }
    
    private List<OrderByClause> parseOrderBy(String orderBy) {
        List<OrderByClause> clauses = new ArrayList<>();
        for (String clause : orderBy.split(",")) {
            String[] parts = clause.trim().split("\\s+");
            String column = parts[0];
            boolean ascending = parts.length == 1 || "ASC".equalsIgnoreCase(parts[1]);
            clauses.add(new OrderByClause(column, ascending));
        }
        return clauses;
    }
    
    private Map<String, String> parseAssignments(String setClause) throws QueryException {
        Map<String, String> assignments = new HashMap<>();
        for (String assignment : setClause.split(",")) {
            String[] parts = assignment.split("=", 2);
            if (parts.length != 2) {
                throw new QueryException("Invalid assignment: " + assignment);
            }
            String column = parts[0].trim();
            String value = parts[1].trim().replaceAll("^['\"]|['\"]$", "");
            assignments.put(column, value);
        }
        return assignments;
    }
}

class QueryOptimizer {
    public OptimizedQuery optimize(ParsedQuery query) {
        ExecutionPlan plan = createExecutionPlan(query);
        return new OptimizedQuery(query, plan);
    }
    
    private ExecutionPlan createExecutionPlan(ParsedQuery query) {
        List<ExecutionStep> steps = new ArrayList<>();
        
        switch (query.type) {
            case SELECT:
                if (query.where != null) {
                    steps.add(new IndexScanStep(query.table, query.where));
                } else {
                    steps.add(new TableScanStep(query.table));
                }
                steps.add(new DeleteStep());
                break;
                
            case BEGIN_TRANSACTION:
            case COMMIT:
            case ROLLBACK:
                steps.add(new TransactionStep(query.type));
                break;
        }
        
        return new ExecutionPlan(steps);
    }
}

class QueryExecutor {
    private final Node node;
    private String currentTransaction;
    
    public QueryExecutor(Node node) {
        this.node = node;
    }
    
    public CompletableFuture<String> execute(OptimizedQuery query) {
        ExecutionContext context = new ExecutionContext();
        context.transactionId = currentTransaction;
        
        return executeSteps(query.plan.steps, context, 0);
    }
    
    private CompletableFuture<String> executeSteps(List<ExecutionStep> steps, ExecutionContext context, int stepIndex) {
        if (stepIndex >= steps.size()) {
            return CompletableFuture.completedFuture(formatResult(context));
        }
        
        ExecutionStep step = steps.get(stepIndex);
        return step.execute(node, context)
            .thenCompose(result -> executeSteps(steps, context, stepIndex + 1));
    }
    
    private String formatResult(ExecutionContext context) {
        if (context.resultRows.isEmpty()) {
            return "No results";
        }
        
        StringBuilder sb = new StringBuilder();
        for (ResultRow row : context.resultRows) {
            sb.append(row.toString()).append("\n");
        }
        return sb.toString().trim();
    }
}

enum QueryType {
    SELECT, INSERT, UPDATE, DELETE, BEGIN_TRANSACTION, COMMIT, ROLLBACK
}

class ParsedQuery {
    final QueryType type;
    final String table;
    final List<String> columns;
    final WhereCondition where;
    final Map<String, String> assignments;
    final List<OrderByClause> orderBy;
    final List<String> values;
    final int limit;
    
    ParsedQuery(QueryType type, String table, List<String> columns, WhereCondition where, 
                Map<String, String> assignments, List<OrderByClause> orderBy, List<String> values, int limit) {
        this.type = type;
        this.table = table;
        this.columns = columns;
        this.where = where;
        this.assignments = assignments;
        this.orderBy = orderBy;
        this.values = values;
        this.limit = limit;
    }
}

class OptimizedQuery {
    final ParsedQuery original;
    final ExecutionPlan plan;
    
    OptimizedQuery(ParsedQuery original, ExecutionPlan plan) {
        this.original = original;
        this.plan = plan;
    }
}

class ExecutionPlan {
    final List<ExecutionStep> steps;
    
    ExecutionPlan(List<ExecutionStep> steps) {
        this.steps = steps;
    }
}

class ExecutionContext {
    final List<ResultRow> resultRows = new ArrayList<>();
    final Map<String, Object> variables = new HashMap<>();
    String transactionId;
    int rowsAffected = 0;
}

class WhereCondition {
    final String column;
    final String operator;
    final String value;
    
    WhereCondition(String column, String operator, String value) {
        this.column = column;
        this.operator = operator;
        this.value = value;
    }
    
    public boolean matches(ResultRow row) {
        String rowValue = row.getValue(column);
        if (rowValue == null) return false;
        
        switch (operator) {
            case "=":
                return rowValue.equals(value);
            case "!=":
                return !rowValue.equals(value);
            case "<":
                return rowValue.compareTo(value) < 0;
            case ">":
                return rowValue.compareTo(value) > 0;
            case "<=":
                return rowValue.compareTo(value) <= 0;
            case ">=":
                return rowValue.compareTo(value) >= 0;
            default:
                return false;
        }
    }
}

class OrderByClause {
    final String column;
    final boolean ascending;
    
    OrderByClause(String column, boolean ascending) {
        this.column = column;
        this.ascending = ascending;
    }
}

class ResultRow {
    private final Map<String, String> values;
    
    public ResultRow() {
        this.values = new HashMap<>();
    }
    
    public void setValue(String column, String value) {
        values.put(column, value);
    }
    
    public String getValue(String column) {
        return values.get(column);
    }
    
    public Map<String, String> getValues() {
        return new HashMap<>(values);
    }
    
    @Override
    public String toString() {
        return values.toString();
    }
}

abstract class ExecutionStep {
    public abstract CompletableFuture<Void> execute(Node node, ExecutionContext context);
}

class TableScanStep extends ExecutionStep {
    private final String table;
    
    TableScanStep(String table) {
        this.table = table;
    }
    
    @Override
    public CompletableFuture<Void> execute(Node node, ExecutionContext context) {
        return CompletableFuture.supplyAsync(() -> {
            List<String> keys = node.getStorage().scan("", "z", 1000);
            for (String key : keys) {
                if (key.startsWith(table + ":")) {
                    byte[] data = node.getStorage().get(key);
                    if (data != null) {
                        ResultRow row = deserializeRow(key, data);
                        context.resultRows.add(row);
                    }
                }
            }
            return null;
        });
    }
    
    private ResultRow deserializeRow(String key, byte[] data) {
        ResultRow row = new ResultRow();
        String[] parts = key.split(":");
        if (parts.length >= 2) {
            row.setValue("id", parts[1]);
        }
        row.setValue("data", new String(data));
        return row;
    }
}

class IndexScanStep extends ExecutionStep {
    private final String table;
    private final WhereCondition condition;
    
    IndexScanStep(String table, WhereCondition condition) {
        this.table = table;
        this.condition = condition;
    }
    
    @Override
    public CompletableFuture<Void> execute(Node node, ExecutionContext context) {
        return CompletableFuture.supplyAsync(() -> {
            String startKey = table + ":" + condition.value;
            String endKey = table + ":" + condition.value + "z";
            
            List<String> keys = node.getStorage().scan(startKey, endKey, 1000);
            for (String key : keys) {
                byte[] data = node.getStorage().get(key);
                if (data != null) {
                    ResultRow row = deserializeRow(key, data);
                    if (condition.matches(row)) {
                        context.resultRows.add(row);
                    }
                }
            }
            return null;
        });
    }
    
    private ResultRow deserializeRow(String key, byte[] data) {
        ResultRow row = new ResultRow();
        String[] parts = key.split(":");
        if (parts.length >= 2) {
            row.setValue("id", parts[1]);
            row.setValue(condition.column, parts[1]);
        }
        row.setValue("data", new String(data));
        return row;
    }
}

class ProjectionStep extends ExecutionStep {
    private final List<String> columns;
    
    ProjectionStep(List<String> columns) {
        this.columns = columns;
    }
    
    @Override
    public CompletableFuture<Void> execute(Node node, ExecutionContext context) {
        return CompletableFuture.supplyAsync(() -> {
            List<ResultRow> projectedRows = new ArrayList<>();
            for (ResultRow row : context.resultRows) {
                ResultRow projectedRow = new ResultRow();
                for (String column : columns) {
                    projectedRow.setValue(column, row.getValue(column));
                }
                projectedRows.add(projectedRow);
            }
            context.resultRows.clear();
            context.resultRows.addAll(projectedRows);
            return null;
        });
    }
}

class SortStep extends ExecutionStep {
    private final List<OrderByClause> orderBy;
    
    SortStep(List<OrderByClause> orderBy) {
        this.orderBy = orderBy;
    }
    
    @Override
    public CompletableFuture<Void> execute(Node node, ExecutionContext context) {
        return CompletableFuture.supplyAsync(() -> {
            context.resultRows.sort((row1, row2) -> {
                for (OrderByClause clause : orderBy) {
                    String val1 = row1.getValue(clause.column);
                    String val2 = row2.getValue(clause.column);
                    
                    if (val1 == null && val2 == null) continue;
                    if (val1 == null) return clause.ascending ? -1 : 1;
                    if (val2 == null) return clause.ascending ? 1 : -1;
                    
                    int cmp = val1.compareTo(val2);
                    if (cmp != 0) {
                        return clause.ascending ? cmp : -cmp;
                    }
                }
                return 0;
            });
            return null;
        });
    }
}

class LimitStep extends ExecutionStep {
    private final int limit;
    
    LimitStep(int limit) {
        this.limit = limit;
    }
    
    @Override
    public CompletableFuture<Void> execute(Node node, ExecutionContext context) {
        return CompletableFuture.supplyAsync(() -> {
            if (context.resultRows.size() > limit) {
                context.resultRows.subList(limit, context.resultRows.size()).clear();
            }
            return null;
        });
    }
}

class InsertStep extends ExecutionStep {
    private final String table;
    private final Map<String, String> assignments;
    
    InsertStep(String table, Map<String, String> assignments) {
        this.table = table;
        this.assignments = assignments;
    }
    
    @Override
    public CompletableFuture<Void> execute(Node node, ExecutionContext context) {
        String id = assignments.getOrDefault("id", UUID.randomUUID().toString());
        String key = table + ":" + id;
        String value = String.join(",", assignments.values());
        
        if (context.transactionId != null) {
            return node.getTransactionManager().write(context.transactionId, key, value.getBytes())
                .thenApply(v -> {
                    context.rowsAffected = 1;
                    return null;
                });
        } else {
            node.getStorage().put(key, value.getBytes());
            context.rowsAffected = 1;
            return CompletableFuture.completedFuture(null);
        }
    }
}

class UpdateStep extends ExecutionStep {
    private final Map<String, String> assignments;
    
    UpdateStep(Map<String, String> assignments) {
        this.assignments = assignments;
    }
    
    @Override
    public CompletableFuture<Void> execute(Node node, ExecutionContext context) {
        return CompletableFuture.supplyAsync(() -> {
            for (ResultRow row : context.resultRows) {
                String key = getKeyFromRow(row);
                String newValue = String.join(",", assignments.values());
                
                if (context.transactionId != null) {
                    node.getTransactionManager().write(context.transactionId, key, newValue.getBytes());
                } else {
                    node.getStorage().put(key, newValue.getBytes());
                }
                context.rowsAffected++;
            }
            return null;
        });
    }
    
    private String getKeyFromRow(ResultRow row) {
        return row.getValue("id");
    }
}

class DeleteStep extends ExecutionStep {
    @Override
    public CompletableFuture<Void> execute(Node node, ExecutionContext context) {
        return CompletableFuture.supplyAsync(() -> {
            for (ResultRow row : context.resultRows) {
                String key = getKeyFromRow(row);
                
                if (context.transactionId != null) {
                    // Transaction delete would need special handling
                } else {
                    node.getStorage().delete(key);
                }
                context.rowsAffected++;
            }
            context.resultRows.clear();
            return null;
        });
    }
    
    private String getKeyFromRow(ResultRow row) {
        return row.getValue("id");
    }
}

class TransactionStep extends ExecutionStep {
    private final QueryType transactionType;
    
    TransactionStep(QueryType transactionType) {
        this.transactionType = transactionType;
    }
    
    @Override
    public CompletableFuture<Void> execute(Node node, ExecutionContext context) {
        switch (transactionType) {
            case BEGIN_TRANSACTION:
                context.transactionId = node.getTransactionManager().beginTransaction();
                break;
            case COMMIT:
                if (context.transactionId != null) {
                    return node.getTransactionManager().commitTransaction(context.transactionId)
                        .thenApply(v -> null);
                }
                break;
            case ROLLBACK:
                if (context.transactionId != null) {
                    return node.getTransactionManager().abortTransaction(context.transactionId)
                        .thenApply(v -> null);
                }
                break;
        }
        return CompletableFuture.completedFuture(null);
    }
}

class QueryException extends Exception {
    QueryException(String message) {
        super(message);
    }
}
                    steps.add(new TableScanStep(query.table));
                }
                
                if (query.columns != null && !query.columns.contains("*")) {
                    steps.add(new ProjectionStep(query.columns));
                }
                
                if (query.orderBy != null) {
                    steps.add(new SortStep(query.orderBy));
                }
                
                if (query.limit > 0) {
                    steps.add(new LimitStep(query.limit));
                }
                break;
                
            case INSERT:
                steps.add(new InsertStep(query.table, query.assignments));
                break;
                
            case UPDATE:
                if (query.where != null) {
                    steps.add(new IndexScanStep(query.table, query.where));
                } else {
                    steps.add(new TableScanStep(query.table));
                }
                steps.add(new UpdateStep(query.assignments));
                break;
                
            case DELETE:
                if (query.where != null) {
                    steps.add(new IndexScanStep(query.table, query.where));
                } else {
