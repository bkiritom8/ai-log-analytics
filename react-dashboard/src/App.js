// react-dashboard/src/App.js
import React, { useState, useEffect, useCallback } from 'react';
import {
  AppBar, Toolbar, Typography, Container, Grid, Paper, Card, CardContent,
  Tab, Tabs, Box, Alert, CircularProgress, Chip, Table, TableBody,
  TableCell, TableContainer, TableHead, TableRow, TextField, Button,
  Switch, FormControlLabel, Badge, Accordion, AccordionSummary, AccordionDetails
} from '@mui/material';
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, PieChart, Pie, Cell, AreaChart, Area, ScatterChart, Scatter
} from 'recharts';
import {
  Dashboard as DashboardIcon, ShowChart as ShowChartIcon, 
  Storage as StorageIcon, Psychology as PsychologyIcon,
  Speed as SpeedIcon, Warning as WarningIcon, Error as ErrorIcon,
  TrendingUp as TrendingUpIcon, ExpandMore as ExpandMoreIcon,
  PlayArrow as PlayArrowIcon, Stop as StopIcon
} from '@mui/icons-material';
import './App.css';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';
const SPARK_API_URL = process.env.REACT_APP_SPARK_API_URL || 'http://localhost:8001';
const WS_URL = process.env.REACT_APP_KAFKA_WS_URL || 'ws://localhost:8000/ws/live-logs';

// Real-time WebSocket hook for Kafka streaming
const useKafkaWebSocket = () => {
  const [socket, setSocket] = useState(null);
  const [messages, setMessages] = useState([]);
  const [connected, setConnected] = useState(false);
  const [stats, setStats] = useState({ total: 0, anomalies: 0, errors: 0 });

  useEffect(() => {
    const ws = new WebSocket(WS_URL);
    
    ws.onopen = () => {
      setConnected(true);
      console.log('🔌 Connected to Kafka WebSocket');
    };
    
    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        
        setMessages(prev => {
          const newMessages = [data, ...prev].slice(0, 1000); // Keep last 1000
          return newMessages;
        });
        
        // Update real-time stats
        setStats(prev => ({
          total: prev.total + 1,
          anomalies: prev.anomalies + (data.is_anomaly ? 1 : 0),
          errors: prev.errors + (data.level === 'ERROR' || data.level === 'FATAL' ? 1 : 0)
        }));
        
      } catch (error) {
        console.error('Failed to parse WebSocket message:', error);
      }
    };
    
    ws.onclose = () => {
      setConnected(false);
      console.log('🔌 Kafka WebSocket disconnected');
    };
    
    ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };
    
    setSocket(ws);
    
    return () => {
      ws.close();
    };
  }, []);

  const sendMessage = useCallback((message) => {
    if (socket && socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify(message));
    }
  }, [socket]);

  return { messages, connected, stats, sendMessage };
};

// Kafka Topic Monitor Component
const KafkaTopicMonitor = () => {
  const [topics, setTopics] = useState([]);
  const [metrics, setMetrics] = useState({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchKafkaData = async () => {
      try {
        // Fetch topics
        const topicsResponse = await fetch(`${API_BASE_URL}/kafka/topics`);
        const topicsData = await topicsResponse.json();
        setTopics(topicsData.topics || []);
        
        // Fetch metrics
        const metricsResponse = await fetch(`${API_BASE_URL}/kafka/metrics`);
        const metricsData = await metricsResponse.json();
        setMetrics(metricsData);
        
      } catch (error) {
        console.error('Failed to fetch Kafka data:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchKafkaData();
    const interval = setInterval(fetchKafkaData, 10000); // Update every 10s
    return () => clearInterval(interval);
  }, []);

  if (loading) return <CircularProgress />;

  return (
    <Grid container spacing={3}>
      {/* Kafka Cluster Overview */}
      <Grid item xs={12} md={4}>
        <Card>
          <CardContent>
            <Typography variant="h6" gutterBottom>
              📊 Kafka Cluster
            </Typography>
            <Typography variant="h4" color="primary">
              {metrics.cluster?.brokers || 0} Brokers
            </Typography>
            <Typography color="textSecondary">
              {topics.length} Topics • {metrics.cluster?.total_partitions || 0} Partitions
            </Typography>
          </CardContent>
        </Card>
      </Grid>
      
      <Grid item xs={12} md={4}>
        <Card>
          <CardContent>
            <Typography variant="h6" gutterBottom>
              🚀 Throughput
            </Typography>
            <Typography variant="h4" color="success.main">
              {(metrics.throughput?.messages_per_sec || 0).toLocaleString()}/s
            </Typography>
            <Typography color="textSecondary">
              {((metrics.throughput?.bytes_per_sec || 0) / 1024 / 1024).toFixed(2)} MB/s
            </Typography>
          </CardContent>
        </Card>
      </Grid>

      <Grid item xs={12} md={4}>
        <Card>
          <CardContent>
            <Typography variant="h6" gutterBottom>
              ⚡ Producer/Consumer
            </Typography>
            <Typography variant="body1">
              Produce: {metrics.throughput?.produce_rate || 0}/s
            </Typography>
            <Typography variant="body1">
              Consume: {metrics.throughput?.consume_rate || 0}/s
            </Typography>
          </CardContent>
        </Card>
      </Grid>

      {/* Topic Details Table */}
      <Grid item xs={12}>
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6" gutterBottom>
            📋 Topic Details
          </Typography>
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Topic Name</TableCell>
                  <TableCell>Partitions</TableCell>
                  <TableCell>Messages/sec</TableCell>
                  <TableCell>Throughput</TableCell>
                  <TableCell>Status</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {topics.map((topic, index) => (
                  <TableRow key={index}>
                    <TableCell>{topic.name}</TableCell>
                    <TableCell>{topic.partitions}</TableCell>
                    <TableCell>
                      {(metrics.topics?.[topic.name]?.messages_per_sec || 0).toLocaleString()}
                    </TableCell>
                    <TableCell>
                      {((metrics.topics?.[topic.name]?.bytes_per_sec || 0) / 1024).toFixed(0)} KB/s
                    </TableCell>
                    <TableCell>
                      <Chip 
                        label={topic.error ? "Error" : "Healthy"} 
                        color={topic.error ? "error" : "success"}
                        size="small"
                      />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </Paper>
      </Grid>
    </Grid>
  );
};

// Real-time Log Stream Component
const RealTimeLogStream = () => {
  const { messages, connected, stats } = useKafkaWebSocket();
  const [filter, setFilter] = useState({ level: '', service: '' });
  const [paused, setPaused] = useState(false);

  const filteredMessages = messages.filter(msg => {
    if (filter.level && msg.level !== filter.level) return false;
    if (filter.service && msg.service !== filter.service) return false;
    return true;
  });

  const getLevelColor = (level) => {
    const colors = {
      DEBUG: '#9e9e9e', INFO: '#2196f3', WARN: '#ff9800', 
      ERROR: '#f44336', FATAL: '#d32f2f'
    };
    return colors[level] || '#9e9e9e';
  };

  return (
    <Box>
      {/* Connection Status and Controls */}
      <Paper sx={{ p: 2, mb: 2 }}>
        <Grid container spacing={2} alignItems="center">
          <Grid item>
            <Chip 
              label={connected ? "🟢 Connected to Kafka" : "🔴 Disconnected"}
              color={connected ? "success" : "error"}
            />
          </Grid>
          <Grid item>
            <Typography variant="body2">
              📨 Total: {stats.total} | 🚨 Anomalies: {stats.anomalies} | ❌ Errors: {stats.errors}
            </Typography>
          </Grid>
          <Grid item>
            <FormControlLabel
              control={<Switch checked={!paused} onChange={(e) => setPaused(!e.target.checked)} />}
              label={paused ? "▶️ Resume" : "⏸️ Pause"}
            />
          </Grid>
        </Grid>
        
        <Grid container spacing={2} sx={{ mt: 1 }}>
          <Grid item xs={6} md={3}>
            <TextField
              fullWidth
              size="small"
              label="Filter by Level"
              value={filter.level}
              onChange={(e) => setFilter(prev => ({...prev, level: e.target.value}))}
            />
          </Grid>
          <Grid item xs={6} md={3}>
            <TextField
              fullWidth
              size="small"
              label="Filter by Service"
              value={filter.service}
              onChange={(e) => setFilter(prev => ({...prev, service: e.target.value}))}
            />
          </Grid>
        </Grid>
      </Paper>

      {/* Real-time Log Stream */}
      <Paper sx={{ p: 2, height: 600, overflow: 'auto' }}>
        <Typography variant="h6" gutterBottom>
          📡 Live Log Stream ({filteredMessages.length} messages)
        </Typography>
        
        {!paused && filteredMessages.map((msg, index) => (
          <Box 
            key={`${msg.timestamp}-${index}`}
            sx={{ 
              borderLeft: `4px solid ${getLevelColor(msg.level)}`,
              pl: 2, py: 1, mb: 1,
              backgroundColor: msg.is_anomaly ? '#fff3e0' : 'inherit',
              border: msg.is_anomaly ? '1px solid #ff9800' : 'none'
            }}
          >
            <Box display="flex" alignItems="center" gap={1}>
              <Chip 
                label={msg.level} 
                size="small"
                style={{ backgroundColor: getLevelColor(msg.level), color: 'white' }}
              />
              <Typography variant="body2" color="textSecondary">
                {new Date(msg.timestamp || msg.log_timestamp).toLocaleTimeString()}
              </Typography>
              <Typography variant="body2" fontWeight="bold">
                {msg.service}
              </Typography>
              {msg.is_anomaly && <Chip label="🚨 ANOMALY" color="warning" size="small" />}
              {msg.severity && (
                <Chip 
                  label={msg.severity} 
                  color={msg.severity === 'CRITICAL' ? 'error' : 'warning'}
                  size="small"
                />
              )}
            </Box>
            <Typography variant="body2" sx={{ mt: 0.5 }}>
              {msg.message || msg.description}
            </Typography>
            {msg.host && (
              <Typography variant="caption" color="textSecondary">
                Host: {msg.host}
              </Typography>
            )}
            {msg.anomaly_score && (
              <Typography variant="caption" color="error.main">
                Anomaly Score: {msg.anomaly_score.toFixed(3)}
              </Typography>
            )}
          </Box>
        ))}
        
        {filteredMessages.length === 0 && (
          <Alert severity="info">
            No messages received yet. Generate test data or check Kafka connection.
          </Alert>
        )}
      </Paper>
    </Box>
  );
};

// Spark Jobs Monitor Component  
const SparkJobsMonitor = () => {
  const [jobs, setJobs] = useState([]);
  const [sparkStats, setSparkStats] = useState({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchSparkData = async () => {
      try {
        const jobsResponse = await fetch(`${SPARK_API_URL}/spark/jobs`);
        const jobsData = await jobsResponse.json();
        setJobs(jobsData.jobs || []);
        
        const statsResponse = await fetch(`${SPARK_API_URL}/spark/cluster-stats`);
        const statsData = await statsResponse.json();
        setSparkStats(statsData);
        
      } catch (error) {
        console.error('Failed to fetch Spark data:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchSparkData();
    const interval = setInterval(fetchSparkData, 15000); // Update every 15s
    return () => clearInterval(interval);
  }, []);

  const submitSparkJob = async (jobType) => {
    try {
      const response = await fetch(`${SPARK_API_URL}/spark/submit`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          job_type: jobType,
          args: jobType === 'streaming' ? ['--master', 'spark://spark-master:7077'] : []
        })
      });
      
      if (response.ok) {
        const data = await response.json();
        alert(`✅ Spark job submitted: ${data.application_id}`);
      }
    } catch (error) {
      alert(`❌ Failed to submit job: ${error.message}`);
    }
  };

  if (loading) return <CircularProgress />;

  return (
    <Grid container spacing={3}>
      {/* Spark Cluster Stats */}
      <Grid item xs={12} md={3}>
        <Card>
          <CardContent>
            <Typography variant="h6">⚡ Spark Cluster</Typography>
            <Typography variant="h4" color="primary">
              {sparkStats.workers || 0} Workers
            </Typography>
            <Typography color="textSecondary">
              {sparkStats.total_cores || 0} cores • {sparkStats.total_memory || '0'} memory
            </Typography>
          </CardContent>
        </Card>
      </Grid>

      <Grid item xs={12} md={3}>
        <Card>
          <CardContent>
            <Typography variant="h6">🏃 Active Jobs</Typography>
            <Typography variant="h4" color="success.main">
              {jobs.filter(job => job.status === 'RUNNING').length}
            </Typography>
            <Typography color="textSecondary">
              {jobs.filter(job => job.status === 'SUCCEEDED').length} completed
            </Typography>
          </CardContent>
        </Card>
      </Grid>

      <Grid item xs={12} md={3}>
        <Card>
          <CardContent>
            <Typography variant="h6">📊 Processing Rate</Typography>
            <Typography variant="h4" color="info.main">
              {sparkStats.records_per_sec || 0}/s
            </Typography>
            <Typography color="textSecondary">
              Avg latency: {sparkStats.avg_latency || 0}ms
            </Typography>
          </CardContent>
        </Card>
      </Grid>

      <Grid item xs={12} md={3}>
        <Card>
          <CardContent>
            <Typography variant="h6">🧠 ML Models</Typography>
            <Typography variant="h4" color="warning.main">
              {sparkStats.active_models || 0}
            </Typography>
            <Typography color="textSecondary">
              Last trained: {sparkStats.last_training || 'Never'}
            </Typography>
          </CardContent>
        </Card>
      </Grid>

      {/* Job Submission Controls */}
      <Grid item xs={12}>
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6" gutterBottom>
            🚀 Submit Spark Jobs
          </Typography>
          <Grid container spacing={2}>
            <Grid item>
              <Button
                variant="contained"
                startIcon={<PlayArrowIcon />}
                onClick={() => submitSparkJob('streaming')}
                color="primary"
              >
                Start Anomaly Detection
              </Button>
            </Grid>
            <Grid item>
              <Button
                variant="contained"
                startIcon={<PsychologyIcon />}
                onClick={() => submitSparkJob('training')}
                color="secondary"
              >
                Train ML Models
              </Button>
            </Grid>
            <Grid item>
              <Button
                variant="contained"
                startIcon={<ShowChartIcon />}
                onClick={() => submitSparkJob('analytics')}
                color="info"
              >
                Run Analytics
              </Button>
            </Grid>
          </Grid>
        </Paper>
      </Grid>

      {/* Active Jobs Table */}
      <Grid item xs={12}>
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6" gutterBottom>
            📋 Spark Applications
          </Typography>
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Application ID</TableCell>
                  <TableCell>Name</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Duration</TableCell>
                  <TableCell>Progress</TableCell>
                  <TableCell>Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {jobs.map((job, index) => (
                  <TableRow key={index}>
                    <TableCell>
                      <Typography variant="body2" fontFamily="monospace">
                        {job.application_id?.slice(-12) || `app-${index}`}
                      </Typography>
                    </TableCell>
                    <TableCell>{job.name || 'ML Training Job'}</TableCell>
                    <TableCell>
                      <Chip 
                        label={job.status || 'RUNNING'}
                        color={
                          job.status === 'RUNNING' ? 'primary' :
                          job.status === 'SUCCEEDED' ? 'success' : 'error'
                        }
                        size="small"
                      />
                    </TableCell>
                    <TableCell>{job.duration || '5m 32s'}</TableCell>
                    <TableCell>
                      <Box display="flex" alignItems="center">
                        <Box width="100%" mr={1}>
                          <div style={{ 
                            width: '100px', 
                            height: '6px', 
                            backgroundColor: '#e0e0e0',
                            borderRadius: '3px'
                          }}>
                            <div style={{
                              width: `${job.progress || 65}%`,
                              height: '100%',
                              backgroundColor: '#1976d2',
                              borderRadius: '3px'
                            }}></div>
                          </div>
                        </Box>
                        <Typography variant="body2">
                          {job.progress || 65}%
                        </Typography>
                      </Box>
                    </TableCell>
                    <TableCell>
                      <Button size="small" color="error">
                        <StopIcon fontSize="small" />
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
                {jobs.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={6}>
                      <Alert severity="info">
                        No active Spark jobs. Submit a job to get started.
                      </Alert>
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </TableContainer>
        </Paper>
      </Grid>
    </Grid>
  );
};

// ML Insights Dashboard Component
const MLInsightsDashboard = () => {
  const [insights, setInsights] = useState({});
  const [anomalies, setAnomalies] = useState([]);
  const [predictions, setPredictions] = useState([]);

  useEffect(() => {
    const fetchMLData = async () => {
      try {
        // Fetch ML insights
        const insightsResponse = await fetch(`${SPARK_API_URL}/ml/insights`);
        const insightsData = await insightsResponse.json();
        setInsights(insightsData);

        // Fetch recent anomalies
        const anomaliesResponse = await fetch(`${API_BASE_URL}/ml/anomalies/recent`);
        const anomaliesData = await anomaliesResponse.json();
        setAnomalies(anomaliesData.anomalies || []);

        // Fetch predictions
        const predictionsResponse = await fetch(`${SPARK_API_URL}/ml/predictions`);
        const predictionsData = await predictionsResponse.json();
        setPredictions(predictionsData.predictions || []);

      } catch (error) {
        console.error('Failed to fetch ML data:', error);
      }
    };

    fetchMLData();
    const interval = setInterval(fetchMLData, 30000); // Update every 30s
    return () => clearInterval(interval);
  }, []);

  // Mock data for demo
  const anomalyTrends = [
    { time: '09:00', anomalies: 12, normal: 1200 },
    { time: '10:00', anomalies: 8, normal: 1150 },
    { time: '11:00', anomalies: 23, normal: 1300 },
    { time: '12:00', anomalies: 45, normal: 1400 },
    { time: '13:00', anomalies: 67, normal: 1250 },
    { time: '14:00', anomalies: 34, normal: 1180 },
  ];

  const serviceAnomalies = [
    { service: 'payment-service', anomalies: 89, severity: 'HIGH' },
    { service: 'user-service', anomalies: 23, severity: 'MEDIUM' },
    { service: 'inventory-service', anomalies: 12, severity: 'LOW' },
    { service: 'notification-service', anomalies: 45, severity: 'MEDIUM' },
  ];

  return (
    <Grid container spacing={3}>
      {/* ML Model Performance */}
      <Grid item xs={12} md={6}>
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6" gutterBottom>
            🧠 ML Model Performance
          </Typography>
          <Grid container spacing={2}>
            <Grid item xs={4}>
              <Box textAlign="center">
                <Typography variant="h4" color="success.main">95.2%</Typography>
                <Typography variant="body2">Anomaly Detection Accuracy</Typography>
              </Box>
            </Grid>
            <Grid item xs={4}>
              <Box textAlign="center">
                <Typography variant="h4" color="info.main">87.8%</Typography>
                <Typography variant="body2">Classification F1-Score</Typography>
              </Box>
            </Grid>
            <Grid item xs={4}>
              <Box textAlign="center">
                <Typography variant="h4" color="warning.main">12ms</Typography>
                <Typography variant="body2">Avg Inference Time</Typography>
              </Box>
            </Grid>
          </Grid>
        </Paper>
      </Grid>

      {/* Predictive Insights */}
      <Grid item xs={12} md={6}>
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6" gutterBottom>
            🔮 Predictive Insights
          </Typography>
          <Box>
            <Alert severity="warning" sx={{ mb: 1 }}>
              🚨 Payment service expected to see 40% increase in errors in next 2 hours
            </Alert>
            <Alert severity="info" sx={{ mb: 1 }}>
              📈 User service traffic projected to peak at 3 PM (+25%)
            </Alert>
            <Alert severity="success">
              💡 Inventory service running optimally, no scaling needed
            </Alert>
          </Box>
        </Paper>
      </Grid>

      {/* Anomaly Trends Chart */}
      <Grid item xs={12} md={8}>
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6" gutterBottom>
            📈 Anomaly Detection Trends
          </Typography>
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={anomalyTrends}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="time" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Area 
                type="monotone" 
                dataKey="normal" 
                stackId="1" 
                stroke="#4caf50" 
                fill="#4caf50" 
                fillOpacity={0.6}
                name="Normal Logs"
              />
              <Area 
                type="monotone" 
                dataKey="anomalies" 
                stackId="1" 
                stroke="#f44336" 
                fill="#f44336"
                name="Anomalies"
              />
            </AreaChart>
          </ResponsiveContainer>
        </Paper>
      </Grid>

      {/* Service Anomaly Breakdown */}
      <Grid item xs={12} md={4}>
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6" gutterBottom>
            🎯 Service Anomaly Breakdown
          </Typography>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={serviceAnomalies}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({service, anomalies}) => `${service}: ${anomalies}`}
                outerRadius={80}
                fill="#8884d8"
                dataKey="anomalies"
              >
                {serviceAnomalies.map((entry, index) => (
                  <Cell 
                    key={`cell-${index}`} 
                    fill={
                      entry.severity === 'HIGH' ? '#f44336' :
                      entry.severity === 'MEDIUM' ? '#ff9800' : '#4caf50'
                    } 
                  />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </Paper>
      </Grid>

      {/* Recent Critical Anomalies */}
      <Grid item xs={12}>
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6" gutterBottom>
            🚨 Recent Critical Anomalies
          </Typography>
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Timestamp</TableCell>
                  <TableCell>Service</TableCell>
                  <TableCell>Severity</TableCell>
                  <TableCell>Description</TableCell>
                  <TableCell>Score</TableCell>
                  <TableCell>Status</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {/* Mock critical anomalies */}
                {[
                  {
                    timestamp: new Date(Date.now() - 300000).toLocaleTimeString(),
                    service: 'payment-service',
                    severity: 'CRITICAL',
                    description: 'Database connection timeout pattern detected',
                    score: 0.97,
                    status: 'INVESTIGATING'
                  },
                  {
                    timestamp: new Date(Date.now() - 600000).toLocaleTimeString(),
                    service: 'user-service', 
                    severity: 'HIGH',
                    description: 'Unusual spike in authentication failures',
                    score: 0.84,
                    status: 'RESOLVED'
                  },
                  {
                    timestamp: new Date(Date.now() - 900000).toLocaleTimeString(),
                    service: 'inventory-service',
                    severity: 'MEDIUM',
                    description: 'Memory usage pattern anomaly',
                    score: 0.72,
                    status: 'MONITORING'
                  }
                ].map((anomaly, index) => (
                  <TableRow key={index}>
                    <TableCell>{anomaly.timestamp}</TableCell>
                    <TableCell>
                      <Chip label={anomaly.service} size="small" variant="outlined" />
                    </TableCell>
                    <TableCell>
                      <Chip 
                        label={anomaly.severity}
                        size="small"
                        color={
                          anomaly.severity === 'CRITICAL' ? 'error' :
                          anomaly.severity === 'HIGH' ? 'warning' : 'info'
                        }
                      />
                    </TableCell>
                    <TableCell>{anomaly.description}</TableCell>
                    <TableCell>
                      <Typography 
                        color={anomaly.score > 0.8 ? 'error.main' : 'warning.main'}
                        fontWeight="bold"
                      >
                        {anomaly.score.toFixed(2)}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Chip 
                        label={anomaly.status}
                        size="small"
                        color={
                          anomaly.status === 'RESOLVED' ? 'success' :
                          anomaly.status === 'INVESTIGATING' ? 'error' : 'default'
                        }
                      />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </Paper>
      </Grid>
    </Grid>
  );
};

// Main App Component
const App = () => {
  const [currentTab, setCurrentTab] = useState(0);
  const [testDataRunning, setTestDataRunning] = useState(false);

  const handleTabChange = (event, newValue) => {
    setCurrentTab(newValue);
  };

  const generateTestData = async () => {
    setTestDataRunning(true);
    try {
      const response = await fetch(`${API_BASE_URL}/logs/generate-test-data`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ count: 5000, rate: 200 })
      });
      
      if (response.ok) {
        alert('✅ Test data generation started! Check the live stream.');
      }
    } catch (error) {
      alert(`❌ Failed to generate test data: ${error.message}`);
    } finally {
      setTimeout(() => setTestDataRunning(false), 25000); // 25 second generation
    }
  };

  const TabPanel = ({ children, value, index }) => (
    <Box hidden={value !== index} sx={{ pt: 3 }}>
      {value === index && children}
    </Box>
  );

  return (
    <Box sx={{ flexGrow: 1 }}>
      <AppBar position="static" sx={{ backgroundColor: '#1976d2' }}>
        <Toolbar>
          <DashboardIcon sx={{ mr: 2 }} />
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
            AI Log Analytics Platform - Kafka + Spark + React
          </Typography>
          <Button 
            color="inherit" 
            onClick={generateTestData}
            disabled={testDataRunning}
            startIcon={testDataRunning ? <CircularProgress size={16} /> : <PlayArrowIcon />}
          >
            {testDataRunning ? 'Generating...' : 'Generate Test Data'}
          </Button>
        </Toolbar>
      </AppBar>

      <Container maxWidth="xl" sx={{ mt: 2, mb: 4 }}>
        <Tabs 
          value={currentTab} 
          onChange={handleTabChange}
          sx={{ borderBottom: 1, borderColor: 'divider', mb: 2 }}
        >
          <Tab 
            label="🔴 Live Stream" 
            icon={<Badge badgeContent="LIVE" color="error"><StorageIcon /></Badge>}
          />
          <Tab label="📊 Kafka Monitor" icon={<SpeedIcon />} />
          <Tab label="⚡ Spark Jobs" icon={<ShowChartIcon />} />
          <Tab label="🧠 ML Insights" icon={<PsychologyIcon />} />
        </Tabs>

        <TabPanel value={currentTab} index={0}>
          <RealTimeLogStream />
        </TabPanel>

        <TabPanel value={currentTab} index={1}>
          <KafkaTopicMonitor />
        </TabPanel>

        <TabPanel value={currentTab} index={2}>
          <SparkJobsMonitor />
        </TabPanel>

        <TabPanel value={currentTab} index={3}>
          <MLInsightsDashboard />
        </TabPanel>
      </Container>
    </Box>
  );
};

export default App;