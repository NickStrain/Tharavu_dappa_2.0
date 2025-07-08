
import React, { useCallback, useEffect, useMemo } from 'react';
import ReactFlow, {
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  addEdge,
  MarkerType,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { motion } from 'framer-motion';
import { Card, CardContent } from '@/components/ui/card';

const NodeGraph = ({ initialNodes, initialEdges, onNodesChangeToParent, onEdgesChangeToParent, onNodeClick }) => {
  const [nodes, setNodes, onNodesChangeInternal] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChangeInternal] = useEdgesState(initialEdges);

  useEffect(() => {
    setNodes(initialNodes);
  }, [initialNodes, setNodes]);

  useEffect(() => {
    setEdges(initialEdges);
  }, [initialEdges, setEdges]);

  const onConnect = useCallback(
    (params) => {
      const newEdge = { 
        ...params, 
        animated: true, 
        style: { stroke: '#8B5CF6' },
        markerEnd: { type: MarkerType.ArrowClosed, color: '#8B5CF6' }
      };
      setEdges((eds) => addEdge(newEdge, eds));
      onEdgesChangeToParent((eds) => addEdge(newEdge, eds));
    },
    [setEdges, onEdgesChangeToParent]
  );
  
  const handleNodesChange = (changes) => {
    onNodesChangeInternal(changes);
    onNodesChangeToParent(changes);
  };

  const handleEdgesChange = (changes) => {
    onEdgesChangeInternal(changes);
    onEdgesChangeToParent(changes);
  };

  const minimapStyle = {
    height: 120,
    backgroundColor: 'rgba(51, 65, 85, 0.7)', 
    borderRadius: '0.375rem', 
    border: '1px solid rgba(100, 116, 139, 0.7)', 
  };

  const nodeColor = (node) => {
    switch (node.type) {
      case 'input': return '#6366F1';
      case 'output': return '#EC4899';
      default: return '#8B5CF6';
    }
  };
  
  const proOptions = { hideAttribution: true };

  return (
    <motion.div
      className="h-full w-full"
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      transition={{ duration: 0.5 }}
    >
      <Card className="h-full bg-slate-800/30 border-purple-500/30 shadow-xl backdrop-blur-sm overflow-hidden">
        <CardContent className="p-0 h-full">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={handleNodesChange}
            onEdgesChange={handleEdgesChange}
            onConnect={onConnect}
            onNodeClick={onNodeClick}
            fitView
            proOptions={proOptions}
            className="bg-transparent"
          >
            <Controls 
              className="[&>button]:bg-slate-700 [&>button]:border-slate-600 [&>button:hover]:bg-slate-600 [&>button_path]:fill-slate-300" 
            />
            <MiniMap nodeColor={nodeColor} style={minimapStyle} pannable zoomable />
            <Background variant="dots" gap={16} size={1} color="rgba(100, 116, 139, 0.4)" />
          </ReactFlow>
        </CardContent>
      </Card>
    </motion.div>
  );
};

export default NodeGraph;
  