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
      ["workflow", "Workflow"],
      ["projects", "Projects"],
      ["tasks", "Tasks"],
      ["council", "Council"],
      ["research", "Research"],
      ["finance", "Finance"],
      ["replay", "Replay"],
      ["system", "System"],
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
    return h("div", {className: "mc-workflow"}, steps.map(function (step) {
      return h("div", {className: "mc-flow-step", key: step.id},
        h("div", {className: "mc-flow-label"}, step.label),
        h("div", {className: "mc-flow-count"}, step.count || 0),
        h(MiniRows, {items: step.detail, limit: 5})
      );
    }));
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
    return h("div", {className: "mc-board-card"},
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
      h("div", {className: "mc-control-row"},
        h(PrioritySelect, {value: card.priority, onChange: function (priority) {
          props.onPriority(card.project_id, priority);
        }})
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
    var lanes = (((props.snapshot || {}).tasks || {}).lanes || []);
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
      h("div", {className: "mc-task-lanes"}, lanes.map(function (lane) {
        return h("section", {className: "mc-lane", key: lane.id},
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
    return h("div", {className: "mc-two-column"},
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
      h(ShellCard, {title: "Task Status"},
        h(MiniRows, {items: summary.tasks_by_status || {}})
      ),
      h(ShellCard, {title: "Recent Briefs", className: "mc-span"},
        h("div", {className: "mc-card-stack"}, (research.recent_briefs || []).length ? research.recent_briefs.map(function (brief) {
          return h("div", {className: "mc-feed-item", key: brief.brief_id},
            h("div", {className: "mc-card-top"}, h("strong", null, brief.title), h(Badge, null, brief.actionability)),
            h("p", null, brief.summary),
            h("small", null, "domain " + brief.domain + " · " + brief.urgency + " · " + pct(brief.confidence))
          );
        }) : h("div", {className: "mc-empty"}, "No briefs"))
      )
    );
  }

  function Finance(props) {
    var finance = (props.snapshot || {}).finance || {};
    var summary = finance.summary || {};
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
      )
    );
  }

  function Replay(props) {
    var replay = (props.snapshot || {}).replay || {};
    var readiness = replay.readiness || {};
    var reliability = replay.reliability || {};
    return h("div", {className: "mc-two-column"},
      h(ShellCard, {title: "Replay Readiness", aside: statusLabel(readiness)},
        h(MetricGrid, {items: [
          ["Eligible", (readiness.eligible_source_traces || 0) + "/" + (readiness.minimum_eligible_traces || 500)],
          ["Known bad", (readiness.known_bad_source_traces || 0) + "/" + (readiness.minimum_known_bad_traces || 25)],
          ["Skills", (readiness.distinct_skill_count || 0) + "/" + (readiness.minimum_distinct_skills || 3)],
          ["Ack below threshold", readiness.operator_ack_required_below_threshold ? "Yes" : "No"]
        ]})
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
      h(ShellCard, {title: "Recent Traces", className: "mc-span"},
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

  function Overview(props) {
    var snapshot = props.snapshot || {};
    var alerts = snapshot.alerts || [];
    var digest = snapshot.latest_digest;
    var posture = snapshot.runtime_posture || {};
    return h("div", {className: "mc-overview-grid"},
      h(ShellCard, {title: "System Pulse", aside: snapshot.generated_at ? SDK.utils.isoTimeAgo(snapshot.generated_at) : "Live"},
        h(MetricGrid, {items: [
          ["Runtime", ((snapshot.overview || {}).runtime_status || {}).lifecycle_state || "UNKNOWN"],
          ["Gates", (snapshot.overview || {}).pending_gates || 0],
          ["Harvests", (snapshot.overview || {}).pending_harvests || 0],
          ["Replay", statusLabel((snapshot.overview || {}).replay_readiness)],
          ["Milestones", statusLabel((snapshot.overview || {}).milestone_health)],
          ["Load", fmt((snapshot.overview || {}).operator_load_hours || 0) + "h"]
        ]})
      ),
      h(ShellCard, {title: "Runtime Posture", aside: posture.substrate || "Hermes"},
        h(MiniRows, {items: [
          ["Mode", posture.mode || "prebuilt"],
          ["Gate writes", posture.gate_actions_enabled ? "enabled" : "read-only"],
          ["Poll interval", (posture.poll_interval_seconds || 15) + "s"],
          ["Heavy services", (posture.heavy_services || []).length]
        ]})
      ),
      h(ShellCard, {title: "Latest Digest"},
        digest ? h("pre", {className: "mc-digest"}, JSON.stringify(digest, null, 2)) : h("div", {className: "mc-empty"}, "No digest yet")
      ),
      h(ShellCard, {title: "Alerts", aside: String(alerts.length)},
        h("div", {className: "mc-card-stack"}, alerts.length ? alerts.map(function (alert) {
          return h("div", {className: "mc-alert", key: alert.alert_id || alert.created_at},
            h("strong", null, alert.alert_type || alert.type || "Alert"),
            h("p", null, alert.content || alert.message || alert.trigger_description || "No message"),
            alert.acknowledged ? null : h(Button, {
              variant: "secondary",
              onClick: function () { props.onAckAlert(alert.alert_id); }
            }, "Acknowledge")
          );
        }) : h("div", {className: "mc-empty"}, "No active alerts"))
      )
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
      if (activeTab === "workflow") return h(Workflow, {snapshot: snapshot});
      if (activeTab === "projects") return h(Board, {
        snapshot: snapshot,
        onProjectPriority: function (projectId, priority) {
          return run(function () { return post("/projects/" + encodeURIComponent(projectId) + "/priority", {priority: priority}); });
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
      if (activeTab === "research") return h(Research, {snapshot: snapshot});
      if (activeTab === "finance") return h(Finance, {snapshot: snapshot});
      if (activeTab === "replay") return h(Replay, {snapshot: snapshot});
      if (activeTab === "system") return h(System, {snapshot: snapshot});
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
          h("p", {className: "mc-subtitle"}, "Hermes-native operator cockpit for strategy, projects, research, finance, replay, and system pressure.")
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
