# StateGraph 与 CompiledStateGraph

::: zerograph.graph.state.StateGraph
    options:
      members:
        - __init__
        - add_node
        - add_edge
        - add_conditional_edges
        - add_sequence
        - set_node_defaults
        - set_entry_point
        - set_conditional_entry_point
        - set_finish_point
        - validate
        - compile
        - get_graph

::: zerograph.graph.state.CompiledStateGraph
    options:
      members:
        - invoke
        - stream
        - ainvoke
        - astream
        - get_state
        - get_state_history
        - update_state
        - batch
        - abatch
        - get_graph
