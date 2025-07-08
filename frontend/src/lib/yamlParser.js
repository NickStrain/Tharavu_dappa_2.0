import yaml from 'js-yaml';

export const parseYamlToElements = (yamlString) => {
  try {
    const doc = yaml.load(yamlString);
    if (!doc || !doc.nodes) {
      return { nodes: [], edges: [] };
    }

    const nodes = [];
    const edges = [];

    Object.entries(doc.nodes).forEach(([nodeId, nodeData], index) => {
      const position = { x: (index % 3) * 250 + 50, y: Math.floor(index / 3) * 150 + 50 };
      nodes.push({
        id: nodeId,
        type: nodeData.type || 'default',
        data: {
          label: `${nodeId} (${nodeData.function || 'N/A'})`,
          ...nodeData.params,
          vars: nodeData.vars || ''
        },
        position: position,
      });
    });

    Object.entries(doc.nodes).forEach(([nodeId, nodeData]) => {
      if (nodeData.dependencies && Array.isArray(nodeData.dependencies)) {
        nodeData.dependencies.forEach(depId => {
          if (doc.nodes[depId]) {
            edges.push({
              id: `e-${depId}-${nodeId}`,
              source: depId,
              target: nodeId,
              animated: true,
              style: { stroke: '#8B5CF6' },
            });
          }
        });
      }
    });

    return { nodes, edges };
  } catch (e) {
    console.error('Error parsing YAML:', e);
    throw new Error(`YAML Parsing Error: ${e.message}`);
  }
};

export const elementsToYaml = (nodes, edges) => {
  const yamlObject = {
    nodes: {},
  };

  nodes.forEach(node => {
    const { id, type, data, ...restNode } = node;
    const { label, vars, ...params } = data;

    const functionName = label ? label.substring(label.indexOf('(') + 1, label.lastIndexOf(')')) : 'unknown_function';

    yamlObject.nodes[id] = {
      function: functionName === 'N/A' ? undefined : functionName,
      params: Object.keys(params).length > 0 ? params : undefined,
      vars: vars || undefined,
      dependencies: [],
      ...restNode,
    };
  });

  edges.forEach(edge => {
    if (yamlObject.nodes[edge.target]) {
      yamlObject.nodes[edge.target].dependencies.push(edge.source);
    }
  });

  for (const nodeId in yamlObject.nodes) {
    if (yamlObject.nodes[nodeId].dependencies.length === 0) {
      delete yamlObject.nodes[nodeId].dependencies;
    }
    if (!yamlObject.nodes[nodeId].function) delete yamlObject.nodes[nodeId].function;
    if (!yamlObject.nodes[nodeId].params) delete yamlObject.nodes[nodeId].params;
    if (!yamlObject.nodes[nodeId].vars) delete yamlObject.nodes[nodeId].vars;

    // Clean up React Flow extra fields
    delete yamlObject.nodes[nodeId].position;
    delete yamlObject.nodes[nodeId].width;
    delete yamlObject.nodes[nodeId].height;
    delete yamlObject.nodes[nodeId].selected;
    delete yamlObject.nodes[nodeId].dragging;
    delete yamlObject.nodes[nodeId].positionAbsolute;
  }

  try {
    return yaml.dump(yamlObject, { skipInvalid: true, indent: 2 });
  } catch (e) {
    console.error("Error converting to YAML:", e);
    return "Error: Could not generate YAML.";
  }
};

export const initialYaml = `
nodes:
  load_data:
    function: read_csv
    params:
      file_path: E:/tharavu-dappa/v2/chennai_reservoir_levels.csv
      separator: ","
    vars: df

  drop_nulls_task:
    function: drop_nans
    params:
      df: df
    vars: df_cleaned
    dependencies:
      - load_data

  rename_task:
    function: rename
    params:
      df: df_cleaned
      mapping: {"POONDI": "PUNDA"}
    vars: renamed_df
    dependencies:
      - drop_nulls_task
`;