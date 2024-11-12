/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.message.Trace;

/**
 * A possible query execution plan. The time required to execute a query depends
 * on the order the tables are accessed.
 */
public class Plan {

    private final TableFilter[] filters;
    private final HashMap<TableFilter, PlanItem> planItems = new HashMap<>();
    private final Expression[] allConditions;
    private final TableFilter[] allFilters;

    /**
     * Create a query plan with the given order.
     *
     * @param filters the tables of the query
     * @param count the number of table items
     * @param condition the condition in the WHERE clause
     */
    public Plan(TableFilter[] filters, int count, Expression condition) { // 表; 表数量; 查询条件
        this.filters = new TableFilter[count];
        System.arraycopy(filters, 0, this.filters, 0, count);
        final ArrayList<Expression> allCond = new ArrayList<>();
        final ArrayList<TableFilter> all = new ArrayList<>();
        if (condition != null) {
            allCond.add(condition); // where 查询条件
        }
        for (int i = 0; i < count; i++) {
            TableFilter f = filters[i];
            f.visit(f1 -> {
                all.add(f1);
                if (f1.getJoinCondition() != null) {
                    allCond.add(f1.getJoinCondition());
                }
            });
        }
        allConditions = allCond.toArray(new Expression[0]);
        allFilters = all.toArray(new TableFilter[0]);
    }

    /**
     * Get the plan item for the given table.
     *
     * @param filter the table
     * @return the plan item
     */
    public PlanItem getItem(TableFilter filter) {
        return planItems.get(filter);
    }

    /**
     * The the list of tables.
     *
     * @return the list of tables
     */
    public TableFilter[] getFilters() {
        return filters;
    }

    /**
     * Remove all index conditions that can not be used.
     */
    public void removeUnusableIndexConditions() {
        for (int i = 0; i < allFilters.length; i++) {
            TableFilter f = allFilters[i];
            setEvaluatable(f, true);
            if (i < allFilters.length - 1) {
                // the last table doesn't need the optimization,
                // otherwise the expression is calculated twice unnecessarily
                // (not that bad but not optimal)
                f.optimizeFullCondition();
            }
            f.removeUnusableIndexConditions();
        }
        for (TableFilter f : allFilters) {
            setEvaluatable(f, false);
        }
    }

    /** 计算查询计划的成本
     * Calculate the cost of this query plan.
     *
     * @param session the session
     * @param allColumnsSet calculates all columns on-demand
     * @return the cost
     */
    public double calculateCost(SessionLocal session, AllColumnsForPlan allColumnsSet) {
        Trace t = session.getTrace();
        if (t.isDebugEnabled()) {
            t.debug("Plan       : calculate cost for plan {0}", Arrays.toString(allFilters));
        }
        double cost = 1; // 初始化成本为1，作为后续成本累乘的基础值
        boolean invalidPlan = false;
        for (int i = 0; i < allFilters.length; i++) { // 遍历所有的表过滤器来计算总成本
            TableFilter tableFilter = allFilters[i];
            if (t.isDebugEnabled()) {
                t.debug("Plan       :   for table filter {0}", tableFilter);
            }
            PlanItem item = tableFilter.getBestPlanItem(session, allFilters, i, allColumnsSet); // 获取最佳的计划项
            planItems.put(tableFilter, item);
            if (t.isDebugEnabled()) {
                t.debug("Plan       :   best plan item cost {0} index {1}",
                        item.cost, item.getIndex().getPlanSQL());
            }
            cost += cost * item.cost; // 更新总成本
            setEvaluatable(tableFilter, true); // 设置表过滤器为可评估状态
            Expression on = tableFilter.getJoinCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) { // 检查连接条件是否可评估
                    invalidPlan = true;
                    break;
                }
            }
        }
        if (invalidPlan) { // 如果计划无效，则设置成本为无穷大
            cost = Double.POSITIVE_INFINITY;
        }
        if (t.isDebugEnabled()) {
            session.getTrace().debug("Plan       : plan cost {0}", cost);
        }
        for (TableFilter f : allFilters) { // 将所有表过滤器重置为不可评估状态
            setEvaluatable(f, false);
        }
        return cost; // 返回最终计算的成本
    }

    private void setEvaluatable(TableFilter filter, boolean b) {
        filter.setEvaluatable(filter, b);
        for (Expression e : allConditions) {
            e.setEvaluatable(filter, b);
        }
    }
}
