(function () {
  var SDK = window.__HERMES_PLUGIN_SDK__;
  var React = SDK.React;
  var h = React.createElement;
  var Card = SDK.components.Card;
  var CardHeader = SDK.components.CardHeader;
  var CardTitle = SDK.components.CardTitle;
  var CardContent = SDK.components.CardContent;
  var Badge = SDK.components.Badge;
  var Button = SDK.components.Button;
  var Input = SDK.components.Input;
  var Separator = SDK.components.Separator;
  var apiBase = "/api/plugins/hybrid-mission-control";
  var priorities = ["P0_IMMEDIATE", "P1_HIGH", "P2_NORMAL", "P3_BACKGROUND"];
  var manualStatuses = ["TODO", "IN_PROGRESS", "BLOCKED", "DONE"];
  var researchWorkflows = [
    ["model_radar", "Model & Tooling Radar"],
    ["system_architecture", "System Architecture"],
    ["business_market", "Business & Opportunity"],
    ["security_compliance", "Security & Compliance"],
    ["operator_prompts", "Operator Prompt"],
    ["standing_monitoring", "Standing Brief"],
    ["harvest_followups", "Harvest Follow-up"]
  ];

  function api(path, options) {
    return SDK.fetchJSON(apiBase + path, options || {});
  }

  function post(path, body) {
    return api(path, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(body || {})
    });
  }

  function fmt(value) {
    if (value === null || value === undefined || value === "") return "None";
    if (typeof value === "number") return Number.isInteger(value) ? String(value) : value.toFixed(2);
    return String(value);
  }

  function money(value) {
    return "$" + fmt(Number(value || 0));
  }

  function pct(value) {
    if (value === null || value === undefined) return "n/a";
    return Math.round(Number(value) * 100) + "%";
  }

  function pressureTone(value) {
    if (value === null || value === undefined) return "unknown";
    if (Number(value) >= 0.8) return "high";
    if (Number(value) >= 0.55) return "medium";
    return "low";
  }

  function statusLabel(value) {
    if (value === null || value === undefined || value === "") return "UNKNOWN";
    var raw = typeof value === "object" ? value.status || value.overall || value.lifecycle_state || "AVAILABLE" : String(value);
    if (raw === "IMPLEMENTED_BELOW_ACTIVATION_THRESHOLD") return "Below Threshold";
    return raw.toLowerCase().split("_").map(function (part) {
      return part.charAt(0).toUpperCase() + part.slice(1);
    }).join(" ");
  }

  function keys(obj) {
    return Object.keys(obj || {});
  }

  function priorityTone(priority) {
    if (priority === "P0_IMMEDIATE") return "mc-priority mc-p0";
    if (priority === "P1_HIGH") return "mc-priority mc-p1";
    if (priority === "P2_NORMAL") return "mc-priority mc-p2";
    return "mc-priority";
  }

  function ShellCard(props) {
    return h(Card, {className: "mc-card " + (props.className || "")},
      props.title ? h(CardHeader, {className: "mc-card-head"},
        h(CardTitle, {className: "mc-card-title"}, props.title),
        props.aside ? h("span", {className: "mc-card-aside"}, props.aside) : null
      ) : null,
      h(CardContent, {className: "mc-card-content"}, props.children)
    );
  }

  function Metric(props) {
    return h("div", {className: "mc-kpi"},
      h("span", null, props.label),
      h("strong", null, props.value),
      props.detail ? h("small", null, props.detail) : null
    );
  }

  function MetricGrid(props) {
    return h("div", {className: "mc-kpis"}, props.items.map(function (item) {
      return h(Metric, {key: item[0], label: item[0], value: item[1], detail: item[2]});
    }));
  }

  function SystemMap(props) {
    var map = (props.snapshot || {}).system_map || {};
    var nodes = map.nodes || [];
    var pressure = map.pressure || {};
    return h(ShellCard, {title: "System Logic Map", aside: "sense -> decide -> build -> operate -> learn"},
      h("div", {className: "mc-system-map"}, nodes.map(function (node, index) {
        return h("div", {className: "mc-map-wrap", key: node.id},
          h("div", {className: "mc-map-node " + (node.state || "quiet")},
            h("span", null, node.label),
            h("strong", null, fmt(node.count || 0)),
            h("small", null, node.detail)
          ),
          index < nodes.length - 1 ? h("span", {className: "mc-map-arrow"}, "->") : null
        );
      })),
      h("div", {className: "mc-pressure-row"},
        h("span", null, "P0 items ", h("strong", null, fmt(pressure.p0_items || 0))),
        h("span", null, "Blocked tasks ", h("strong", null, fmt(pressure.blocked_tasks || 0))),
        h("span", null, "Pending decisions ", h("strong", null, fmt(pressure.pending_decisions || 0)))
      )
    );
  }

  function FocusQueue(props) {
    var focus = (props.snapshot || {}).operator_focus || {};
    var items = []
      .concat((focus.decisions || []).map(function (item) {
        return {type: item.kind || "Decision", title: item.title, detail: item.target, priority: item.priority};
      }))
      .concat((focus.projects || []).map(function (item) {
        return {type: "Project", title: item.title, detail: item.focus_note || item.lane, priority: item.priority};
      }))
      .concat((focus.tasks || []).map(function (item) {
        return {type: item.source || "Task", title: item.title, detail: item.lane, priority: item.priority};
      }));
    return h(ShellCard, {title: "Operator Focus", aside: String(items.length)},
      h("div", {className: "mc-focus-list"}, items.length ? items.slice(0, 10).map(function (item, index) {
        return h("div", {className: "mc-focus-item", key: item.type + item.title + index},
          h("div", null,
            h("span", {className: priorityTone(item.priority)}, item.priority || "P2_NORMAL"),
            h("strong", null, item.title || "Untitled")
          ),
          h("small", null, item.type + " · " + (item.detail || "No detail"))
        );
      }) : h("div", {className: "mc-empty"}, "No priority focus items"))
    );
  }

  function ResourcePressure(props) {
    var resources = (props.snapshot || {}).resource_pressure || {};
    var items = [resources.cpu, resources.gpu, resources.ram].filter(Boolean);
    return h(ShellCard, {title: "Local Resource Pressure", aside: "lightweight sample"},
      h("div", {className: "mc-resource-grid"}, items.map(function (item) {
        var tone = pressureTone(item.pressure);
        return h("div", {className: "mc-resource " + tone, key: item.label},
          h("div", {className: "mc-card-top"},
            h("span", null, item.label),
            h("strong", null, pct(item.pressure))
          ),
          h("div", {className: "mc-resource-bar"},
            h("span", {style: {width: item.pressure === null || item.pressure === undefined ? "0%" : pct(item.pressure)}})
          ),
          h("small", null, item.detail || "No sample")
        );
      }))
    );
  }

  function SystemStatusStrip(props) {
    var areas = (props.snapshot || {}).area_status || [];
    var resources = (props.snapshot || {}).resource_pressure || {};
    var usage = (props.snapshot || {}).usage || {};
    var tokens = usage.tokens || {};
    var worst = areas.some(function (area) { return area.state === "red"; }) ? "red" :
      (areas.some(function (area) { return area.state === "yellow"; }) ? "yellow" : "green");
    return h("div", {className: "mc-status-strip"},
      h("div", {className: "mc-status-main"},
        h("span", {className: "mc-status-light " + worst}),
        h("strong", null, worst === "green" ? "System flowing" : (worst === "yellow" ? "Operator decision needed" : "System blocked")),
        h("small", null, areas.filter(function (area) { return area.operator_needed; }).length + " areas need attention")
      ),
      h("div", {className: "mc-status-meters"},
        h("span", null, "CPU ", h("strong", null, pct((resources.cpu || {}).pressure))),
        h("span", null, "GPU ", h("strong", null, pct((resources.gpu || {}).pressure))),
        h("span", null, "RAM ", h("strong", null, pct((resources.ram || {}).pressure))),
        h("span", null, "Tokens ", h("strong", null, tokens.tracked ? fmt(tokens.total || 0) : "n/a"))
      )
    );
  }

  function AreaStatusGrid(props) {
    var areas = (props.snapshot || {}).area_status || [];
    return h("div", {className: "mc-area-grid"}, areas.map(function (area) {
      var models = area.models || [];
      return h("section", {className: "mc-area-card " + area.state, key: area.name},
        h("div", {className: "mc-card-top"},
          h("div", {className: "mc-area-title"},
            h("span", {className: "mc-status-light " + area.state}),
            h("strong", null, area.name)
          ),
          area.operator_needed ? h("span", {className: "mc-pill danger"}, "decision needed") : h("span", {className: "mc-pill"}, area.state)
        ),
        h("p", null, area.detail),
        h(MiniRows, {items: [
          ["Active", area.active || 0],
          ["Pending", area.pending || 0],
          ["Blocked", area.blocked || 0]
        ], limit: 3}),
        h("div", {className: "mc-model-stack"},
          h("small", null, "Models in motion"),
          models.length ? models.map(function (model, index) {
            return h("div", {className: "mc-model-row", key: area.name + model.role + index},
              h("span", null, model.role),
              h("strong", null, model.model || "unassigned"),
              h("em", null, (model.route || "route") + " · " + fmt(model.count || 0))
            );
          }) : h("span", {className: "mc-muted"}, "No live route telemetry yet")
        )
      );
    }));
  }

  function MiniRows(props) {
    var source = props.items || {};
    var rows = Array.isArray(source) ? source : keys(source).map(function (key) {
      return [key, source[key]];
    });
    return h("div", {className: "mc-mini-list"}, rows.length ? rows.slice(0, props.limit || 8).map(function (row, index) {
      return h("div", {className: "mc-mini-row", key: row[0] + index},
        h("span", null, row[0]),
        h("strong", null, fmt(row[1]))
      );
    }) : h("span", {className: "mc-muted"}, "No records"));
  }

  function SectionTabs(props) {
    var tabs = [
      ["overview", "Overview"],
      ["projects", "Projects"],
      ["tasks", "Tasks"],
      ["council", "Council"],
      ["research", "Research"],
      ["finance", "Finance"],
      ["self_improvement", "Self-Improve"],
      ["decisions", "Decisions"]
    ];
    return h("div", {className: "mc-tabs"}, tabs.map(function (tab) {
      return h("button", {
        key: tab[0],
        className: props.active === tab[0] ? "mc-tab active" : "mc-tab",
        onClick: function () { props.onChange(tab[0]); }
      }, tab[1]);
    }));
  }

  function Workflow(props) {
    var steps = (((props.snapshot || {}).workflow || {}).steps || []);
    return h("div", {className: "mc-workflow-view"},
      h(SystemMap, {snapshot: props.snapshot}),
      h("div", {className: "mc-workflow"}, steps.map(function (step) {
        return h("div", {className: "mc-flow-step", key: step.id},
          h("div", {className: "mc-flow-label"}, step.label),
          h("div", {className: "mc-flow-count"}, step.count || 0),
          h(MiniRows, {items: step.detail, limit: 5})
        );
      }))
    );
  }

  function PrioritySelect(props) {
    return h("select", {
      className: "mc-select",
      value: props.value || "P3_BACKGROUND",
      onChange: function (event) { props.onChange(event.target.value); }
    }, priorities.map(function (priority) {
      return h("option", {key: priority, value: priority}, priority);
    }));
  }

  function ProjectCard(props) {
    var card = props.card;
    return h("form", {className: "mc-board-card", onSubmit: function (event) {
        event.preventDefault();
        var data = new FormData(event.currentTarget);
        props.onPriority(card.project_id, data.get("priority"), data.get("focus_note"));
      }},
      h("div", {className: "mc-card-top"},
        h("span", {className: priorityTone(card.priority)}, card.priority),
        card.pending_gate_count ? h("span", {className: "mc-pill danger"}, card.pending_gate_count + " gate") : null
      ),
      h("h4", null, card.name),
      h("p", null, card.thesis || "No thesis recorded."),
      h("div", {className: "mc-meta-grid"},
        h("span", null, "Phase"), h("strong", null, card.phase_name || card.status),
        h("span", null, "Cashflow"), h("strong", null, money(card.cashflow_actual_usd || 0)),
        h("span", null, "Burn"), h("strong", null, card.executor_burn_ratio === null ? "n/a" : pct(card.executor_burn_ratio))
      ),
      h("label", {className: "mc-field"},
        h("span", null, "Priority"),
        h("select", {name: "priority", className: "mc-select", defaultValue: card.priority || "P3_BACKGROUND"},
          priorities.map(function (priority) { return h("option", {key: priority, value: priority}, priority); })
        )
      ),
      h("label", {className: "mc-field"},
        h("span", null, "Focus note"),
        h("textarea", {name: "focus_note", defaultValue: card.focus_note || "", placeholder: "Why this matters now"})
      ),
      h("div", {className: "mc-control-row"},
        h(Button, {type: "submit", variant: "secondary"}, "Set focus")
      )
    );
  }

  function Board(props) {
    var lanes = (((props.snapshot || {}).project_board || {}).lanes || []);
    return h("div", {className: "mc-board"}, lanes.filter(function (lane) {
      return lane.count > 0 || ["PIPELINE", "BUILD", "OPERATE", "KILL_REVIEW"].indexOf(lane.id) !== -1;
    }).map(function (lane) {
      return h("section", {className: "mc-lane", key: lane.id},
        h("h3", null, h("span", null, lane.label), h("small", null, lane.count)),
        h("div", {className: "mc-card-stack"}, (lane.cards || []).length ? lane.cards.map(function (card) {
          return h(ProjectCard, {key: card.project_id, card: card, onPriority: props.onProjectPriority});
        }) : h("div", {className: "mc-empty"}, "Nothing here yet"))
      );
    }));
  }

  function TaskCard(props) {
    var task = props.task;
    return h("div", {className: "mc-task"},
      h("div", {className: "mc-card-top"},
        h(Badge, null, task.source),
        h("span", {className: priorityTone(task.priority)}, task.priority)
      ),
      h("h4", null, task.title),
      h("p", null, task.details || "No details."),
      h("div", {className: "mc-control-row"},
        h(PrioritySelect, {value: task.priority, onChange: function (priority) {
          props.onPriority(task, priority);
        }}),
        task.kind === "manual" ? h("select", {
          className: "mc-select",
          value: task.status,
          onChange: function (event) { props.onManualStatus(task.id, event.target.value); }
        }, manualStatuses.map(function (status) {
          return h("option", {key: status, value: status}, status);
        })) : null
      )
    );
  }

  function Tasks(props) {
    var boards = (((props.snapshot || {}).tasks || {}).workflow_boards || []);
    return h("div", {className: "mc-task-layout"},
      h("form", {className: "mc-task-form", onSubmit: props.onCreateManualTask},
        h("h3", null, "Add Operator Task"),
        h(Input, {name: "title", placeholder: "What needs operator attention?", required: true}),
        h("textarea", {name: "details", placeholder: "Optional detail"}),
        h("div", {className: "mc-control-row"},
          h("select", {name: "priority", className: "mc-select", defaultValue: "P2_NORMAL"},
            priorities.map(function (priority) { return h("option", {key: priority, value: priority}, priority); })
          ),
          h(Button, {type: "submit"}, "Add task")
        )
      ),
      h("div", {className: "mc-task-workflows"}, boards.map(function (board) {
        return h("section", {className: "mc-task-board", key: board.id},
          h("div", {className: "mc-card-top"},
            h("div", null,
              h("h3", null, board.label),
              h("p", null, board.purpose)
            ),
            h("span", {className: "mc-pill"}, board.count)
          ),
          h("div", {className: "mc-task-lanes"}, (board.lanes || []).map(function (lane) {
            return h("section", {className: "mc-lane", key: board.id + lane.id},
              h("h3", null, h("span", null, lane.label), h("small", null, lane.count)),
              h("div", {className: "mc-card-stack"}, (lane.cards || []).length ? lane.cards.map(function (task) {
                return h(TaskCard, {
                  key: task.kind + ":" + task.id,
                  task: task,
                  onPriority: props.onTaskPriority,
                  onManualStatus: props.onManualStatus
                });
              }) : h("div", {className: "mc-empty"}, "No tasks"))
            );
          }))
        );
      }))
    );
  }

  function Council(props) {
    var council = (props.snapshot || {}).council || {};
    var summary = council.summary || {};
    return h("div", {className: "mc-two-column"},
      h(ShellCard, {title: "Council Signal", aside: "bounded"},
        h(MetricGrid, {items: [
          ["Verdicts", summary.total_verdicts || 0],
          ["Tier 2", summary.tier2_verdicts || 0],
          ["Degraded", summary.degraded_verdicts || 0],
          ["Avg confidence", pct(summary.avg_confidence)],
          ["DA quality", pct(summary.avg_da_quality)],
          ["Tier 2 G3", summary.pending_tier2_g3 || 0]
        ]})
      ),
      h(ShellCard, {title: "Decision Mix"},
        h(MiniRows, {items: council.by_decision_type || {}})
      ),
      h(ShellCard, {title: "Recent Verdicts", className: "mc-span"},
        h("div", {className: "mc-card-stack"}, (council.recent_verdicts || []).length ? council.recent_verdicts.map(function (item) {
          return h("div", {className: "mc-feed-item", key: item.verdict_id},
            h("div", {className: "mc-card-top"}, h("strong", null, item.decision_type), h(Badge, null, item.recommendation)),
            h("p", null, item.reasoning_summary),
            h("small", null, "tier " + item.tier_used + " · confidence " + pct(item.confidence))
          );
        }) : h("div", {className: "mc-empty"}, "No council verdicts"))
      )
    );
  }

  function Research(props) {
    var research = (props.snapshot || {}).research || {};
    var summary = research.summary || {};
    var lifecycle = research.model_lifecycle || {};
    var conversion = research.conversion_flow || {};
    return h("div", {className: "mc-research-layout"},
      h("form", {className: "mc-research-form", onSubmit: props.onCreateResearchTask},
        h("h3", null, "New Research Task"),
        h("label", {className: "mc-field"},
          h("span", null, "Type"),
          h("select", {name: "workflow_id", className: "mc-select", defaultValue: "operator_prompts"},
            researchWorkflows.map(function (workflow) { return h("option", {key: workflow[0], value: workflow[0]}, workflow[1]); })
          )
        ),
        h(Input, {name: "title", placeholder: "Research question or task", required: true}),
        h("textarea", {name: "brief", placeholder: "What should the researcher answer, and what would count as useful output?"}),
        h("div", {className: "mc-control-row"},
          h("select", {name: "priority", className: "mc-select", defaultValue: "P2_NORMAL"},
            priorities.map(function (priority) { return h("option", {key: priority, value: priority}, priority); })
          ),
          h("select", {name: "depth", className: "mc-select", defaultValue: "QUICK"},
            h("option", {value: "QUICK"}, "QUICK"),
            h("option", {value: "FULL"}, "FULL")
          )
        ),
        h(Button, {type: "submit"}, "Create research task")
      ),
      h(ShellCard, {title: "Research Load"},
        h(MetricGrid, {items: [
          ["Briefs", summary.briefs_total || 0],
          ["Actionable", summary.actionable_briefs || 0],
          ["Quality holds", summary.quality_holds || 0],
          ["Confidence", pct(summary.avg_brief_confidence)],
          ["Harvests", summary.pending_harvests || 0],
          ["Standing", summary.active_standing_briefs || 0]
        ]})
      ),
      h(ShellCard, {title: "Research to Opportunity Flow", className: "mc-span"},
        h("div", {className: "mc-conversion-flow"}, (conversion.stages || []).map(function (stage, index) {
          return h("div", {className: "mc-conversion-wrap", key: stage.id},
            h("div", {className: "mc-conversion-stage"},
              h("span", null, stage.label),
              h("strong", null, fmt(stage.count || 0)),
              h("small", null, stage.detail)
            ),
            index < (conversion.stages || []).length - 1 ? h("span", {className: "mc-map-arrow"}, "->") : null
          );
        })),
        h("div", {className: "mc-card-stack"}, (conversion.actionable_briefs || []).length ? conversion.actionable_briefs.map(function (brief) {
          return h("div", {className: "mc-feed-item", key: brief.brief_id},
            h("div", {className: "mc-card-top"},
              h("strong", null, brief.title),
              h(Badge, null, brief.actionability)
            ),
            h("p", null, brief.summary),
            h("small", null, ((research.domain_labels || {})[String(brief.domain)] || ("Domain " + brief.domain)) + " · " + brief.action_type + " · " + pct(brief.confidence))
          );
        }) : h("div", {className: "mc-empty"}, "No actionable research findings yet"))
      ),
      h(ShellCard, {title: "Research Workflow Lanes", className: "mc-span"},
        h("div", {className: "mc-research-workflows"}, (research.workflows || []).map(function (workflow) {
          return h("section", {className: "mc-research-workflow", key: workflow.id},
            h("div", {className: "mc-card-top"},
              h("h3", null, workflow.label),
              workflow.p0_p1 ? h("span", {className: "mc-pill danger"}, workflow.p0_p1 + " high") : h("span", {className: "mc-pill"}, workflow.active + " active")
            ),
            h("p", null, workflow.purpose),
            h(MiniRows, {items: [
              ["Total", workflow.total || 0],
              ["Active", workflow.active || 0],
              ["Blocked", workflow.blocked || 0]
            ], limit: 3}),
            h("div", {className: "mc-research-task-list"}, (workflow.tasks || []).length ? workflow.tasks.map(function (task) {
              return h("div", {className: "mc-research-task", key: task.task_id},
                h("div", {className: "mc-card-top"},
                  h("strong", null, task.title),
                  h("span", {className: priorityTone(task.priority)}, task.priority)
                ),
                h("small", null, task.domain_label + " · " + task.source + " · " + task.status)
              );
            }) : h("div", {className: "mc-empty"}, "No work in this lane"))
          );
        }))
      ),
      h(ShellCard, {title: "Domain & Source Split"},
        h("div", {className: "mc-card-stack"},
          h("div", null,
            h("strong", null, "Task Status"),
            h(MiniRows, {items: summary.tasks_by_status || {}})
          ),
          h("div", null,
            h("strong", null, "Task Source"),
            h(MiniRows, {items: summary.tasks_by_source || {}})
          )
        )
      ),
      h(ShellCard, {title: "Model Lifecycle"},
        h(MiniRows, {items: [
          ["Scouted", lifecycle.scouted || 0],
          ["Assessed", lifecycle.assessed || 0],
          ["Shadow trials", lifecycle.shadow_trials || 0]
        ]})
      ),
      h(ShellCard, {title: "Recent Briefs", className: "mc-span"},
        h("div", {className: "mc-card-stack"}, (research.recent_briefs || []).length ? research.recent_briefs.map(function (brief) {
          return h("div", {className: "mc-feed-item", key: brief.brief_id},
            h("div", {className: "mc-card-top"}, h("strong", null, brief.title), h(Badge, null, brief.actionability)),
            h("p", null, brief.summary),
            h("small", null, ((research.domain_labels || {})[String(brief.domain)] || ("Domain " + brief.domain)) + " · " + brief.urgency + " · " + pct(brief.confidence))
          );
        }) : h("div", {className: "mc-empty"}, "No briefs"))
      )
    );
  }

  function Finance(props) {
    var finance = (props.snapshot || {}).finance || {};
    var summary = finance.summary || {};
    var usage = (props.snapshot || {}).usage || {};
    var tokens = usage.tokens || {};
    var traces = usage.traces || {};
    return h("div", {className: "mc-two-column"},
      h(ShellCard, {title: "Financial Posture", aside: "$0 autonomous spend"},
        h(MetricGrid, {items: [
          ["Revenue", money(summary.total_revenue_usd)],
          ["Cost", money(summary.total_cost_usd)],
          ["Cloud", money(summary.cloud_cost_usd)],
          ["Net", money(summary.net_usd)],
          ["Disputed", money(summary.disputed_cost_usd)],
          ["Paid enabled", summary.autonomous_paid_spend_enabled ? "Yes" : "No"]
        ]})
      ),
      h(ShellCard, {title: "Route Mix"},
        h("div", {className: "mc-card-stack"}, (finance.route_mix || []).length ? finance.route_mix.map(function (route) {
          return h("div", {className: "mc-mini-row", key: route.route},
            h("span", null, route.route),
            h("strong", null, route.count + " · " + money(route.cost_usd))
          );
        }) : h("div", {className: "mc-empty"}, "No routing decisions"))
      ),
      h(ShellCard, {title: "Project P&L", className: "mc-span"},
        h("div", {className: "mc-card-stack"}, (finance.project_pnl || []).length ? finance.project_pnl.map(function (item) {
          return h("div", {className: "mc-feed-item", key: item.project_id},
            h("div", {className: "mc-card-top"}, h("strong", null, item.name), h(Badge, null, money(item.net_to_date))),
            h(MiniRows, {items: [["Revenue", money(item.revenue_to_date)], ["Direct cost", money(item.direct_cost)]]})
          );
        }) : h("div", {className: "mc-empty"}, "No active project P&L"))
      ),
      h(ResourcePressure, {snapshot: props.snapshot}),
      h(ShellCard, {title: "Token Accounting", aside: tokens.tracked ? "trace payloads" : "not attached"},
        h(MetricGrid, {items: [
          ["Total", tokens.tracked ? fmt(tokens.total || 0) : "n/a"],
          ["Input", tokens.tracked ? fmt(tokens.tokens_in || 0) : "n/a"],
          ["Output", tokens.tracked ? fmt(tokens.tokens_out || 0) : "n/a"],
          ["Records", tokens.records || 0]
        ]}),
        h("p", {className: "mc-usage-note"}, tokens.note || "No token accounting note.")
      ),
      h(ShellCard, {title: "Trace Usage", className: "mc-span"},
        h(MetricGrid, {items: [
          ["Traces", traces.count || 0],
          ["Trace cost", money(traces.cost_usd || 0)],
          ["Runtime", fmt(Math.round((traces.duration_ms || 0) / 1000)) + "s"]
        ]})
      ),
      h(ShellCard, {title: "Route Usage", className: "mc-span"},
        h("div", {className: "mc-card-stack"}, (usage.routes || []).length ? usage.routes.map(function (route) {
          return h("div", {className: "mc-mini-row", key: route.route},
            h("span", null, route.route),
            h("strong", null, route.count + " · " + money(route.cost_usd))
          );
        }) : h("div", {className: "mc-empty"}, "No routing usage yet"))
      )
    );
  }

  function SelfImprovement(props) {
    var replay = (props.snapshot || {}).replay || {};
    var readiness = replay.readiness || {};
    var reliability = replay.reliability || {};
    return h("div", {className: "mc-two-column"},
      h(ShellCard, {title: "Hermes Harness Readiness", aside: statusLabel(readiness)},
        h(MetricGrid, {items: [
          ["Eligible", (readiness.eligible_source_traces || 0) + "/" + (readiness.minimum_eligible_traces || 500)],
          ["Known bad", (readiness.known_bad_source_traces || 0) + "/" + (readiness.minimum_known_bad_traces || 25)],
          ["Skills", (readiness.distinct_skill_count || 0) + "/" + (readiness.minimum_distinct_skills || 3)],
          ["Ack below threshold", readiness.operator_ack_required_below_threshold ? "Yes" : "No"]
        ]}),
        h("p", {className: "mc-usage-note"}, "This is the self-improvement harness: Hermes traces work, judges outcomes, builds replay coverage, and eventually uses that evidence to propose safer skill or prompt variants.")
      ),
      h(ShellCard, {title: "Reliability Watch"},
        h("div", {className: "mc-card-stack"},
          (reliability.critical_steps || []).length ? (reliability.critical_steps || []).map(function (step) {
            return h("div", {className: "mc-mini-row", key: step.step_type + step.skill},
              h("span", null, step.step_type + " / " + step.skill),
              h("strong", null, pct(step.reliability_7d))
            );
          }) : h("div", {className: "mc-empty"}, "No critical reliability rows")
        )
      ),
      h(ShellCard, {title: "Harness Trace Feed", className: "mc-span"},
        h("div", {className: "mc-card-stack"}, (replay.recent_traces || []).length ? replay.recent_traces.map(function (trace) {
          return h("div", {className: "mc-feed-item", key: trace.trace_id},
            h("div", {className: "mc-card-top"}, h("strong", null, trace.skill_name), h(Badge, null, trace.judge_verdict)),
            h("p", null, trace.intent_goal),
            h("small", null, "score " + fmt(trace.outcome_score) + " · " + fmt(trace.duration_ms) + "ms")
          );
        }) : h("div", {className: "mc-empty"}, "No execution traces"))
      )
    );
  }

  function System(props) {
    var system = (props.snapshot || {}).system || {};
    var alerts = (props.snapshot || {}).alerts || [];
    var runtime = system.runtime_control || {};
    var breaker = system.circuit_breakers || {};
    return h("div", {className: "mc-two-column"},
      h(ShellCard, {title: "Runtime Control"},
        h(MetricGrid, {items: [
          ["Lifecycle", runtime.lifecycle_state || "UNKNOWN"],
          ["Heartbeat", system.heartbeat_state || "UNKNOWN"],
          ["Digest", system.recommended_digest_type || "daily"],
          ["Blocked restarts", runtime.blocked_restart_attempts || 0],
          ["Judge", (system.judge_deadlock || {}).mode || "UNKNOWN"],
          ["Quarantines", ((system.quarantined_responses || {}).pending_review_count || 0)]
        ]})
      ),
      h(ShellCard, {title: "Circuit Breakers"},
        h(MiniRows, {items: [
          ["Critical", (breaker.critical || []).length],
          ["Degraded", (breaker.degraded || []).length],
          ["Active", (breaker.logged_active || []).length],
          ["T3 alerts", breaker.unacknowledged_t3_alerts || 0],
          ["Operator overload", breaker.operator_overload ? "Yes" : "No"]
        ]})
      ),
      h(ShellCard, {title: "Database Contracts", className: "mc-span"},
        h(MiniRows, {items: system.db_status || {}})
      ),
      h(ShellCard, {title: "System Alerts", aside: String(alerts.length), className: "mc-span"},
        h("div", {className: "mc-card-stack"}, alerts.length ? alerts.map(function (alert) {
          return h("div", {className: "mc-alert", key: alert.alert_id || alert.created_at},
            h("strong", null, alert.alert_type || alert.type || "Alert"),
            h("p", null, alert.content || alert.message || alert.trigger_description || "No message"),
            alert.acknowledged ? null : h(Button, {
              variant: "secondary",
              onClick: function () { props.onAckAlert(alert.alert_id); }
            }, "Acknowledge")
          );
        }) : h("div", {className: "mc-empty"}, "No active system alerts"))
      )
    );
  }

  function Decisions(props) {
    var decisions = ((props.snapshot || {}).decisions || {});
    return h("div", {className: "mc-decision-grid"},
      decisionList("Pending Gates", decisions.pending_gates || [], function (item) {
        return [item.gate_type, item.trigger_description, item.project_name || item.project_id || "No project"];
      }),
      decisionList("G3 Spend Requests", decisions.pending_g3_requests || [], function (item) {
        return [item.request_id || item.approval_id || "G3", item.justification || item.reason || "Approval required", "Read-only until dashboard gate validation"];
      }),
      decisionList("Quarantines", decisions.pending_quarantines || [], function (item) {
        return [item.quarantine_id || "Quarantine", item.reason || item.source_breaker || "Pending review", "Read-only until dashboard gate validation"];
      }),
      decisionList("Runtime Halts", decisions.runtime_halts || [], function (item) {
        return [item.halt_id || item.event_id || "Runtime halt", item.halt_reason || item.reason || "Active halt", item.status || "ACTIVE"];
      })
    );
  }

  function Usage(props) {
    var usage = (props.snapshot || {}).usage || {};
    var tokens = usage.tokens || {};
    var traces = usage.traces || {};
    return h("div", {className: "mc-two-column"},
      h(ResourcePressure, {snapshot: props.snapshot}),
      h(ShellCard, {title: "Token Accounting", aside: tokens.tracked ? "trace payloads" : "not attached"},
        h(MetricGrid, {items: [
          ["Total", tokens.tracked ? fmt(tokens.total || 0) : "n/a"],
          ["Input", tokens.tracked ? fmt(tokens.tokens_in || 0) : "n/a"],
          ["Output", tokens.tracked ? fmt(tokens.tokens_out || 0) : "n/a"],
          ["Records", tokens.records || 0]
        ]}),
        h("p", {className: "mc-usage-note"}, tokens.note || "No token accounting note.")
      ),
      h(ShellCard, {title: "Trace Usage", className: "mc-span"},
        h(MetricGrid, {items: [
          ["Traces", traces.count || 0],
          ["Trace cost", money(traces.cost_usd || 0)],
          ["Runtime", fmt(Math.round((traces.duration_ms || 0) / 1000)) + "s"]
        ]})
      ),
      h(ShellCard, {title: "Route Usage", className: "mc-span"},
        h("div", {className: "mc-card-stack"}, (usage.routes || []).length ? usage.routes.map(function (route) {
          return h("div", {className: "mc-mini-row", key: route.route},
            h("span", null, route.route),
            h("strong", null, route.count + " · " + money(route.cost_usd))
          );
        }) : h("div", {className: "mc-empty"}, "No routing usage yet"))
      )
    );
  }

  function decisionList(title, items, mapper) {
    return h(ShellCard, {title: title, aside: String(items.length)},
      h("div", {className: "mc-card-stack"}, items.length ? items.map(function (item, index) {
        var parts = mapper(item);
        return h("div", {className: "mc-decision", key: parts[0] + index},
          h("strong", null, parts[0]),
          h("p", null, parts[1]),
          h("small", null, parts[2])
        );
      }) : h("div", {className: "mc-empty"}, "Clear"))
    );
  }

  function FlowModelList(props) {
    var models = props.models || [];
    return h("div", {className: "mc-flow-models"},
      h("small", null, "Model selection"),
      models.length ? models.map(function (model, index) {
        return h("div", {className: "mc-flow-model", key: (model.role || "role") + index},
          h("span", null, model.role || "Role"),
          h("strong", null, model.model || "unassigned"),
          h("em", null, (model.route || "route") + " · " + fmt(model.count || 0) + " runs")
        );
      }) : h("span", {className: "mc-muted"}, "No route telemetry yet")
    );
  }

  function FlowStage(props) {
    var stage = props.stage || {};
    return h("section", {className: "mc-flow-card " + (stage.status || "quiet")},
      h("div", {className: "mc-card-top"},
        h("div", {className: "mc-area-title"},
          h("span", {className: "mc-status-light " + (stage.status === "attention" ? "yellow" : (stage.status === "blocked" ? "red" : "green"))}),
          h("strong", null, stage.label)
        ),
        h("span", {className: "mc-pill"}, statusLabel(stage.status || "quiet"))
      ),
      h("div", {className: "mc-flow-count-row"},
        h("strong", null, fmt(stage.count || 0)),
        h("span", null, stage.detail)
      ),
      h(MiniRows, {items: [
        ["Pending", stage.pending || 0],
        ["Blocked", stage.blocked || 0]
      ], limit: 2}),
      h(FlowModelList, {models: stage.models || []})
    );
  }

  function OverviewFlow(props) {
    var flow = (props.snapshot || {}).overview_flow || {};
    var summary = flow.summary || {};
    var status = flow.status || "unknown";
    var statusText = status === "operator_needed" ? "Operator decision needed" : (status === "blocked" ? "System blocked" : "System flowing");
    return h("div", {className: "mc-overview-flow"},
      h("div", {className: "mc-status-strip mc-flow-strip"},
        h("div", {className: "mc-status-main"},
          h("span", {className: "mc-status-light " + (status === "operator_needed" ? "yellow" : (status === "blocked" ? "red" : "green"))}),
          h("strong", null, statusText),
          h("small", null, fmt(summary.pending_decisions || 0) + " decisions · " + fmt(summary.follow_up_research || 0) + " research follow-ups")
        ),
        h("div", {className: "mc-status-meters"},
          h("span", null, "Research ", h("strong", null, fmt(summary.active_research || 0))),
          h("span", null, "Findings ", h("strong", null, fmt(summary.actionable_findings || 0))),
          h("span", null, "Opportunities ", h("strong", null, fmt(summary.opportunity_candidates || 0))),
          h("span", null, "Backlog ", h("strong", null, fmt(summary.backlog_items || 0)))
        )
      ),
      h(ShellCard, {title: "Intelligence to Action Flow", aside: "research -> findings -> opportunity"},
        h("div", {className: "mc-flow-main"}, (flow.main_stages || []).map(function (stage, index) {
          return h("div", {className: "mc-flow-main-wrap", key: stage.id},
            h(FlowStage, {stage: stage}),
            index < (flow.main_stages || []).length - 1 ? h("span", {className: "mc-map-arrow"}, "->") : null
          );
        }))
      ),
      h(ShellCard, {title: "Routing Outcomes", aside: "what happens next"},
        h("div", {className: "mc-flow-branches"}, (flow.branch_stages || []).map(function (stage) {
          return h(FlowStage, {key: stage.id, stage: stage});
        }))
      )
    );
  }

  function Overview(props) {
    var snapshot = props.snapshot || {};
    return h("div", {className: "mc-overview-grid"},
      h(OverviewFlow, {snapshot: snapshot}),
      h(FocusQueue, {snapshot: snapshot})
    );
  }

  function MissionControl() {
    var useState = SDK.hooks.useState;
    var useEffect = SDK.hooks.useEffect;
    var state = useState(null);
    var snapshot = state[0];
    var setSnapshot = state[1];
    var tabState = useState("overview");
    var activeTab = tabState[0];
    var setActiveTab = tabState[1];
    var errorState = useState(null);
    var error = errorState[0];
    var setError = errorState[1];

    function refresh() {
      return api("/snapshot").then(function (payload) {
        setSnapshot(payload);
        setError(null);
      }).catch(function (err) {
        setError(String(err.message || err));
      });
    }

    useEffect(function () {
      refresh();
      var timer = setInterval(refresh, 15000);
      return function () { clearInterval(timer); };
    }, []);

    function run(action) {
      return action().then(refresh).catch(function (err) {
        setError(String(err.message || err));
      });
    }

    function body() {
      if (!snapshot) return h("div", {className: "mc-loading"}, "Loading Mission Control...");
      if (activeTab === "projects") return h(Board, {
        snapshot: snapshot,
        onProjectPriority: function (projectId, priority, focusNote) {
          return run(function () {
            return post("/projects/" + encodeURIComponent(projectId) + "/priority", {
              priority: priority,
              focus_note: focusNote || ""
            });
          });
        }
      });
      if (activeTab === "tasks") return h(Tasks, {
        snapshot: snapshot,
        onCreateManualTask: function (event) {
          event.preventDefault();
          var form = event.currentTarget;
          var data = new FormData(form);
          return run(function () {
            return post("/manual-tasks", {
              title: data.get("title"),
              details: data.get("details"),
              priority: data.get("priority")
            });
          }).then(function () { form.reset(); });
        },
        onTaskPriority: function (task, priority) {
          if (task.kind === "manual") {
            return run(function () { return post("/manual-tasks/" + encodeURIComponent(task.id), {priority: priority}); });
          }
          return run(function () { return post("/tasks/priority", {kind: task.kind, id: task.id, priority: priority}); });
        },
        onManualStatus: function (taskId, status) {
          return run(function () { return post("/manual-tasks/" + encodeURIComponent(taskId), {status: status}); });
        }
      });
      if (activeTab === "council") return h(Council, {snapshot: snapshot});
      if (activeTab === "research") return h(Research, {
        snapshot: snapshot,
        onCreateResearchTask: function (event) {
          event.preventDefault();
          var form = event.currentTarget;
          var data = new FormData(form);
          return run(function () {
            return post("/research-tasks", {
              workflow_id: data.get("workflow_id"),
              title: data.get("title"),
              brief: data.get("brief"),
              priority: data.get("priority"),
              depth: data.get("depth"),
              source: "operator"
            });
          }).then(function () { form.reset(); });
        }
      });
      if (activeTab === "finance") return h(Finance, {snapshot: snapshot});
      if (activeTab === "self_improvement") return h(SelfImprovement, {snapshot: snapshot});
      if (activeTab === "decisions") return h(Decisions, {snapshot: snapshot});
      return h(Overview, {
        snapshot: snapshot,
        onAckAlert: function (alertId) {
          if (!alertId) return Promise.resolve();
          return run(function () { return post("/alerts/" + encodeURIComponent(alertId) + "/ack", {}); });
        }
      });
    }

    return h("div", {className: "mc-root"},
      h("header", {className: "mc-hero"},
        h("div", null,
          h("p", {className: "mc-eyebrow"}, "Hybrid Autonomous AI"),
          h("h1", null, "Mission Control"),
          h("p", {className: "mc-subtitle"}, "Hermes-native operator cockpit that shows how the system senses, decides, builds, operates, and learns.")
        ),
        h("div", {className: "mc-hero-note"},
          h("strong", null, "Final plugin shape"),
          h("span", null, "No bundled React, no Node bridge, no live stream server. Gate and quarantine decisions remain read-only.")
        )
      ),
      error ? h("div", {className: "mc-error"}, error) : null,
      h(SectionTabs, {active: activeTab, onChange: setActiveTab}),
      h(Separator, {className: "mc-separator"}),
      body()
    );
  }

  window.__HERMES_PLUGINS__.register("hybrid-mission-control", MissionControl);
})();
