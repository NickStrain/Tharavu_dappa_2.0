
import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { X, Save } from 'lucide-react';

const ParameterEditor = ({ node, onUpdateNode, onClose }) => {
  const [params, setParams] = useState({});
  const [nodeId, setNodeId] = useState('');
  const [nodeFunction, setNodeFunction] = useState('');

  useEffect(() => {
    if (node && node.data) {
      const { label, ...restData } = node.data;
      setParams(restData || {});
      setNodeId(node.id);

      if (label && typeof label === 'string') {
        const funcMatch = label.match(/\(([^)]+)\)/);
        setNodeFunction(funcMatch ? funcMatch[1] : 'N/A');
      } else {
        setNodeFunction('N/A');
      }

    } else {
      setParams({});
      setNodeId('');
      setNodeFunction('');
    }
  }, [node]);

  const handleChange = (key, value) => {
    setParams(prevParams => ({ ...prevParams, [key]: value }));
  };

  const handleAddParam = () => {
    const newKey = `newParam${Object.keys(params).length + 1}`;
    setParams(prevParams => ({ ...prevParams, [newKey]: '' }));
  };

  const handleSave = () => {
    if (node) {
      const updatedNodeData = {
        ...node.data,
        ...params,
      };
      onUpdateNode(node.id, updatedNodeData);
    }
  };
  
  if (!node) {
    return (
      <motion.div
        initial={{ opacity: 0, x: 50 }}
        animate={{ opacity: 1, x: 0 }}
        exit={{ opacity: 0, x: 50 }}
        transition={{ duration: 0.3 }}
        className="w-full md:w-1/3 lg:w-1/4 p-1"
      >
        <Card className="bg-slate-800/50 border-purple-500/50 shadow-xl backdrop-blur-md h-full">
          <CardHeader>
            <CardTitle className="text-xl font-semibold text-purple-300">Node Parameters</CardTitle>
            <CardDescription className="text-slate-400">Select a node to view and edit its parameters.</CardDescription>
          </CardHeader>
          <CardContent>
             <div className="flex items-center justify-center h-40">
                <p className="text-slate-500">No node selected.</p>
             </div>
          </CardContent>
        </Card>
      </motion.div>
    );
  }

  return (
    <motion.div
      key={node.id} 
      initial={{ opacity: 0, x: 50 }}
      animate={{ opacity: 1, x: 0 }}
      exit={{ opacity: 0, x: 50 }}
      transition={{ duration: 0.3 }}
      className="w-full md:w-1/3 lg:w-1/4 p-1"
    >
      <Card className="bg-slate-800/50 border-purple-500/50 shadow-xl backdrop-blur-md">
        <CardHeader className="flex flex-row items-start justify-between">
          <div>
            <CardTitle className="text-xl font-semibold text-purple-300">Edit: {nodeId}</CardTitle>
            <CardDescription className="text-slate-400">Function: {nodeFunction}</CardDescription>
          </div>
          <Button variant="ghost" size="icon" onClick={onClose} className="text-slate-400 hover:text-slate-200">
            <X className="h-5 w-5" />
          </Button>
        </CardHeader>
        <CardContent className="space-y-4 max-h-[calc(100vh-350px)] overflow-y-auto p-4">
          {Object.entries(params).map(([key, value]) => (
            <div key={key} className="space-y-1">
              <Label htmlFor={`${node.id}-${key}`} className="text-sm font-medium text-slate-300">{key}</Label>
              <Input
                id={`${node.id}-${key}`}
                type="text"
                value={value}
                onChange={(e) => handleChange(key, e.target.value)}
                className="bg-slate-700/50 border-slate-600 text-slate-200 focus:ring-pink-500 focus:border-pink-500"
              />
            </div>
          ))}
          <div className="flex flex-col space-y-2 pt-2">
             <Button onClick={handleAddParam} variant="outline" className="w-full bg-blue-500/20 hover:bg-blue-500/40 border-blue-500 text-blue-300">
                Add Parameter
             </Button>
             <Button onClick={handleSave} className="w-full bg-gradient-to-r from-purple-500 to-pink-500 hover:from-purple-600 hover:to-pink-600 text-white">
                <Save className="mr-2 h-4 w-4" /> Save Changes
             </Button>
          </div>
        </CardContent>
      </Card>
    </motion.div>
  );
};

export default ParameterEditor;
  