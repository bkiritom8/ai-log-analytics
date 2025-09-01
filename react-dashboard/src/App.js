// react-dashboard/src/App.js (FIXED VERSION)
import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  AppBar, Toolbar, Typography, Container, Grid, Paper, Card, CardContent,
  Tab, Tabs, Box, Alert, CircularProgress, Chip, Table, TableBody,
  TableCell, TableContainer, TableHead, TableRow, TextField, Button,
  Switch, FormControlLabel, Badge, Accordion, AccordionSummary, AccordionDetails,
  Snackbar
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
  PlayArrow as PlayArrowIcon, Stop as StopIcon, Refresh as RefreshIcon
} from '@mui/icons-material';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';
const SPARK_API_URL = process.env.REACT_APP_SPARK_API_URL || 'http://localhost:8001';
const WS_URL = process.env.REACT_APP_KAFKA_WS_URL || 'ws://localhost:8000/ws/live-logs';

// Enhanced WebSocket hook with reconnection and error handling
const useKafkaWebSocket = () => {
  const [socket, setSocket] = useState(null);
  const [messages, setMessages] = useState([]);
  const [connected, setConnected] = useState(false);
  const [stats, setStats] = useState({ total: 0, anomalies: 0, errors: 0 });
  const [reconnectAttempts, setReconnectAttempts] = useState(0);
  const [error, setError] = useState(null);
  const reconnectTimeoutRef = useRef(null);
  const maxReconnectAttempts = 5;

  const connect = useCallback(() => {
    try {
      const ws = new WebSocket(WS_URL);
      
      ws.onopen = () => {
        setConnected(true);
        setReconnectAttempts(0);
        setError(null);
        console.log('🔌 Connected to Kafka WebSocket');
        
        // Send ping to keep connection alive
        const pingInterval = setInterval(() => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'ping' }));
          } else {
            clearInterval(pingInterval);
          }
        }, 30000); // Ping every 30 seconds
      };
      
      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          
          // Handle different message types
          if (data.type === 'pong' || data.type === 'ping') {
            return; // Ignore keep-alive messages
          }
          
          setMessages(prev => {
            const newMessages = [data, ...prev].slice(0, 1000); // Keep last 1000
            return newMessages;
          });
          
          // Update real-time stats
          setStats(prev => ({
            total: prev.total + 1,
            anomalies: prev.anomalies + (data.is_anomaly || data.severity ? 1 : 0),
            errors: prev.errors + (data.level === 'ERROR' || data.level === 'FATAL' ? 1 : 0)
          }));
          
        } catch (error) {
          console.error('Failed to parse WebSocket message:', error);
          setError('Failed to parse incoming message');
        }
      };
      
      ws.onclose = (event) => {
        setConnected(false);
        setSocket(null);
        
        // Auto-reconnect with exponential backoff
        if (reconnectAttempts < maxReconnectAttempts) {
          const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);
          console.log(`🔄 Reconnecting in ${delay}ms (attempt ${reconnectAttempts + 1}/${maxReconnectAttempts})`);
          
          reconnectTimeoutRef.current = setTimeout(() => {
            setReconnectAttempts(prev => prev + 1);
            connect();
          }, delay);
        } else {
          setError('Max reconnection attempts reached');
          console.error('🔴 Max WebSocket reconnection attempts reached');
        }
      };
      
      ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        setError('WebSocket connection error');
      };
      
      setSocket(ws);
      
    } catch (error) {
      console.error('Failed to create WebSocket:', error);
      setError('Failed to create WebSocket connection');
    }
  }, [reconnectAttempts]);

  useEffect(() => {
    connect();
    
    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (socket) {
        socket.close();
      }
    };
  }, []); // Only run once on mount

  const sendMessage = useCallback((message) => {
    if (socket && socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify(message));
    } else {
      setError('WebSocket not connected');
    }
  }, [socket]);

  const resetConnection = useCallback(() => {
    setReconnectAttempts(0);
    setError(null);
    if (socket) {
      socket.close();
    }
    connect();
  }, [connect, socket]);

  return { messages, connected, stats, sendMessage, error, resetConnection };
};

// Enhanced Kafka Topic Monitor Component
const KafkaTopicMonitor = () => {
  const [topics, setTopics] = useState([]);
  const [metrics, setMetrics] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchKafkaData = useCallback(async () => {
    try {
      setError(null);
      
      // Fetch topics with timeout
      const topicsController = new AbortController();
      const topicsTimeout = setTimeout(() => topicsController.abort(), 10000);
      
      const topicsResponse = await fetch(`${API_BASE_URL}/kafka/topics`, {
        signal: topicsController.signal
      });
      clearTimeout(topicsTimeout);
      
      if (!topicsResponse.ok) {
        throw new Error(`HTTP ${topicsResponse.status}: ${topicsResponse.statusText}`);
      }
      
      const topicsData = await topicsResponse.json();
      setTopics(topicsData.topics || []);
      
      // Fetch metrics
      const metricsController = new AbortController();
      const metricsTimeout = setTimeout(() => metricsController.abort(), 10000);
      
      const metricsResponse = await fetch(`${API_BASE_URL}/kafka/metrics`, {
        signal: metricsController.signal
      });
      clearTimeout(metricsTimeout);
      
      if (!metricsResponse.ok) {
        throw new Error(`HTTP ${metricsResponse.status}: ${metricsResponse.statusText}`);
      }
      
      const metricsData = await metricsResponse.json();
      setMetrics(metricsData);
      
    } catch (error) {
      console.error('Failed to fetch Kafka data:', error);
      setError(error.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchKafkaData();
    const interval = setInterval(fetchKafkaData, 10000); // Update every 10s
    return () => clearInterval(interval);
  }, [fetchKafkaData]);

  if (loading) return <Box display="flex" justifyContent="center" p={4}><CircularProgress /></Box>;
  
  if (error) {
    return (
      <Alert severity="error" action={
        <Button color="inherit" size="small" onClick={fetchKafkaData}>
          <RefreshIcon />
        </Button>
      }>
        Failed to load Kafka data: {error}
      </Alert>
    );
  }

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
            <Chip 
              label={metrics.cluster?.status || "Unknown"} 
              color={metrics.cluster?.status === "healthy" ? "success" : "warning"}
              size="small"
              sx={{ mt: 1 }}
            />
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
            <Typography variant="h6" gutterBottom">
              ⚡ Producer/Consumer
            </Typography>
            <Typography variant="body1">
              Produce: {(metrics.throughput?.produce_rate || 0).toLocaleString()}/s
            </Typography>
            <Typography variant="body1">
              Consume: {(metrics.throughput?.consume_rate || 0).toLocaleString()}/s
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
                  <TableCell>Consumer Lag</TableCell>
                  <TableCell>Status</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {topics.map((topic, index) => (
                  <TableRow key={index}>
                    <TableCell>
                      <Typography variant="body2" fontFamily="monospace">
                        {topic.name}
                      </Typography>
                    </TableCell>
                    <TableCell>{topic.partitions || topic.partition_count}</TableCell>
                    <TableCell>
                      {(metrics.topics?.[topic.name]?.messages_per_sec || 0).toLocaleString()}
                    </TableCell>
                    <TableCell>
                      {((metrics.topics?.[topic.name]?.bytes_per_sec || 0) / 1024).toFixed(0)} KB/s
                    </TableCell>
                    <TableCell>
                      <Typography 
                        color={
                          (metrics.topics?.[topic.name]?.consumer_lag || 0) > 100 ? 
                          "error.main" : "success.main"
                        }
                      >
                        {metrics.topics?.[topic.name]?.consumer_lag || 0}
                      </Typography>
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
                {topics.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={6}>
                      <Alert severity="info">
                        No topics found. Check Kafka connection.
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

// Enhanced Real-time Log Stream Component
const RealTimeLogStream = () => {
  const { messages, connected, stats, error, resetConnection } = useKafkaWebSocket();
  const [filter, setFilter] = useState({ level: '', service: '' });
  const [paused, setPaused] = useState(false);
  const [displayedMessages, setDisplayedMessages] = useState([]);
  const [showError, setShowError] = useState(false);

  // Update displayed messages when not paused
  useEffect(() => {
    if (!paused) {
      setDisplayedMessages(messages);
    }
  }, [messages, paused]);

  // Show error notification
  useEffect(() => {
    if (error) {
      setShowError(true);
    }
  }, [error]);

  const filteredMessages = displayedMessages.filter(msg => {
    if (filter.level && msg.level !== filter.level) return false;
    if (filter.service && msg.service !== filter.service) return false;
    return true;
  });

  const getLevelColor = (level) => {
    const colors = {
      DEBUG: '#9e9e9e', 
      INFO: '#2196f3', 
      WARN: '#ff9800', 
      ERROR: '#f44336', 
      FATAL: '#d32f2f'
    };
    return colors[level] || '#9e9e9e';
  };

  const formatTimestamp = (timestamp) => {
    try {
      return new Date(timestamp).toLocaleTimeString();
    } catch (e) {
      return 'Invalid time';
    }
  };

  return (
    <Box>
      {/* Error Snackbar */}
      <Snackbar
        open={showError}
        autoHideDuration={6000}
        onClose={() => setShowError(false)}
      >
        <Alert 
          onClose={() => setShowError(false)} 
          severity="error"
          action={
            <Button color="inherit" size="small" onClick={resetConnection}>
              Reconnect
            </Button>
          }
        >
          WebSocket Error: {error}
        </Alert>
      </Snackbar>

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
              📨 Total: {stats.total.toLocaleString()} | 🚨 Anomalies: {stats.anomalies.toLocaleString()} | ❌ Errors: {stats.errors.toLocaleString()}
            </Typography>
          </Grid>
          <Grid item>
            <FormControlLabel
              control={
                <Switch 
                  checked={!paused} 
                  onChange={(e) => setPaused(!e.target.checked)}
                  color="primary"
                />
              }
              label={paused ? "▶️ Resume" : "⏸️ Pause"}
            />
          </Grid>
          {!connected && (
            <Grid item>
              <Button 
                size="small" 
                variant="outlined" 
                onClick={resetConnection}
                startIcon={<RefreshIcon />}
              >
                Reconnect
              </Button>
            </Grid>
          )}
        </Grid>
        
        <Grid container spacing={2} sx={{ mt: 1 }}>
          <Grid item xs={6} md={3}>
            <TextField
              fullWidth
              size="small"
              label="Filter by Level"
              value={filter.level}
              onChange={(e) => setFilter(prev => ({...prev, level: e.target.value}))}
              select
              SelectProps={{ native: true }}
            >
              <option value="">All Levels</option>
              <option value="DEBUG">DEBUG</option>
              <option value="INFO">INFO</option>
              <option value="WARN">WARN</option>
              <option value="ERROR">ERROR</option>
              <option value="FATAL">FATAL</option>
            </TextField>
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
          <Grid item xs={12} md={6}>
            <Typography variant="body2" color="textSecondary">
              Showing {filteredMessages.length} of {displayedMessages.length} messages
              {paused && " (paused)"}
            </Typography>
          </Grid>
        </Grid>
      </Paper>

      {/* Real-time Log Stream */}
      <Paper sx={{ p: 2, height: 600, overflow: 'auto' }}>
        <Typography variant="h6" gutterBottom>
          📡 Live Log Stream
        </Typography>
        
        {filteredMessages.length > 0 ? (
          <Box>
            {filteredMessages.map((msg, index) => (
              <Box 
                key={`${msg.timestamp || msg.log_timestamp}-${index}`}
                sx={{ 
                  borderLeft: `4px solid ${getLevelColor(msg.level)}`,
                  pl: 2, py: 1, mb: 1,
                  backgroundColor: (msg.is_anomaly || msg.severity) ? '#fff3e0' : 'inherit',
                  border: (msg.is_anomaly || msg.severity) ? '1px solid #ff9800' : 'none',
                  borderRadius: 1,
                  '&:hover': {
                    backgroundColor: '#f5f5f5'
                  }
                }}
              >
                <Box display="flex" alignItems="center" gap={1} flexWrap="wrap">
                  <Chip 
                    label={msg.level} 
                    size="small"
                    style={{ 
                      backgroundColor: getLevelColor(msg.level), 
                      color: 'white',
                      fontWeight: 'bold'
                    }}
                  />
                  <Typography variant="body2" color="textSecondary">
                    {formatTimestamp(msg.timestamp || msg.log_timestamp || msg.detection_timestamp)}
                  </Typography>
                  <Chip 
                    label={msg.service} 
                    size="small" 
                    variant="outlined"
                    color="primary"
                  />
                  {(msg.is_anomaly || msg.severity) && (
                    <Chip 
                      label={`🚨 ${msg.severity || 'ANOMALY'}`} 
                      color="warning" 
                      size="small" 
                    />
                  )}
                  {msg.anomaly_score && (
                    <Chip 
                      label={`Score: ${parseFloat(msg.anomaly_score).toFixed(2)}`}
                      color="error"
                      size="small"
                      variant="outlined"
                    />
                  )}
                </Box>
                <Typography variant="body2" sx={{ mt: 0.5, wordBreak: 'break-word' }}>
                  {msg.message || msg.description}
                </Typography>
                {(msg.host || msg.trace_id) && (
                  <Box sx={{ mt: 0.5 }}>
                    {msg.host && (
                      <Typography variant="caption" color="textSecondary" sx={{ mr: 2 }}>
                        Host: {msg.host}
                      </Typography>
                    )}
                    {msg.trace_id && (
                      <Typography variant="caption" color="textSecondary" fontFamily="monospace">
                        Trace: {msg.trace_id}
                      </Typography>
                    )}
                  </Box>
                )}
              </Box>
            ))}
          </Box>
        ) : (
          <Alert severity="info">
            {connected ? 
              "No messages received yet. Generate test data or check Kafka topics." :
              "Disconnected from Kafka stream. Attempting to reconnect..."
            }
          </Alert>
        )}
      </Paper>
    </Box>
  );
};

// Enhanced Spark Jobs Monitor Component  
const SparkJobsMonitor = () => {
  const [jobs, setJobs] = useState([]);
  const [sparkStats, setSparkStats] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchSparkData = useCallback(async () => {
    try {
      setError(null);
      
      // Fetch jobs with timeout
      const jobsController = new AbortController();
      const jobsTimeout = setTimeout(() => jobsController.abort(), 15000);
      
      const jobsResponse = await fetch(`${SPARK_API_URL}/spark/jobs`, {
        signal: jobsController.signal
      });
      clearTimeout(jobsTimeout);
      
      if (jobsResponse.ok) {
        const jobsData = await jobsResponse.json();
        setJobs(jobsData.jobs || []);
      } else {
        throw new Error(`Jobs API: HTTP ${jobsResponse.status}`);
      }
      
      // Fetch stats
      const statsController = new AbortController();
      const statsTimeout = setTimeout(() => statsController.abort(), 15000);
      
      const statsResponse = await fetch(`${SPARK_API_URL}/spark/cluster-stats`, {
        signal: statsController.signal
      });
      clearTimeout(statsTimeout);
      
      if (statsResponse.ok) {
        const statsData = await statsResponse.json();
        setSparkStats(statsData);
      } else {
        throw new Error(`Stats API: HTTP ${statsResponse.status}`);
      }
      
    } catch (error) {
      console.error('Failed to fetch Spark data:', error);
      setError(error.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchSparkData();
    const interval = setInterval(fetchSparkData, 15000); // Update every 15s
    return () => clearInterval(interval);
  }, [fetchSparkData]);

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
        fetchSparkData(); // Refresh data
      } else {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
    } catch (error) {
      alert(`❌ Failed to submit job: ${error.message}`);
    }
  };

  if (loading) return <Box display="flex" justifyContent="center" p={4}><CircularProgress /></Box>;

  return (
    <Grid container spacing={3}>
      {error && (
        <Grid item xs={12}>
          <Alert severity="warning" action={
            <Button color="inherit" size="small" onClick={fetchSparkData}>
              <RefreshIcon />
            </Button>
          }>
            Spark API connection issues: {error}. Using cached data.
          </Alert>
        </Grid>
      )}

      {/* Spark Cluster Stats */}
      <Grid item xs={12} md={3}>
        <Card>
          <CardContent>
            <Typography variant="h6">⚡ Spark Cluster</Typography>
            <Typography variant="h4" color="primary">
              {sparkStats.workers || 0} Workers
            </Typography>
            <Typography color="textSecondary">
              {sparkStats.total_cores || 0} cores • {sparkStats.total_memory || '0G'} memory
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
              {(sparkStats.records_per_sec || 0).toLocaleString()}/s
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
                disabled={loading}
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
                disabled={loading}
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
                disabled={loading}
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
                        <Box width="100px" mr={1}>
                          <div style={{ 
                            width: '100%', 
                            height: '8px', 
                            backgroundColor: '#e0e0e0',
                            borderRadius: '4px'
                          }}>
                            <div style={{
                              width: `${job.progress || 65}%`,
                              height: '100%',
                              backgroundColor: job.status === 'FAILED' ? '#f44336' : '#1976d2',
                              borderRadius: '4px',
                              transition: 'width 0.3s ease'
                            }}></div>
                          </div>
                        </Box>
                        <Typography variant="body2" fontWeight="bold">
                          {job.progress || 65}%
                        </Typography>
                      </Box>
                    </TableCell>
                    <TableCell>
                      <Button 
                        size="small" 
                        color="error"
                        disabled={job.status !== 'RUNNING'}
                        startIcon={<StopIcon />}
                      >
                        Stop
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

// Enhanced ML Insights Dashboard Component
const MLInsightsDashboard = () => {
  const [insights, setInsights] = useState({});
  const [anomalies, setAnomalies] = useState([]);
  const [predictions, setPredictions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchMLData = useCallback(async () => {
    try {
      setError(null);
      
      // Fetch recent anomalies
      const anomaliesResponse = await fetch(`${API_BASE_URL}/ml/anomalies/recent?limit=10`);
      if (anomaliesResponse.ok) {
        const anomaliesData = await anomaliesResponse.json();
        setAnomalies(anomaliesData.anomalies || []);
      }

      // Mock additional ML data for demo
      setInsights({
        model_accuracy: 0.952,
        detection_latency: 12,
        false_positive_rate: 0.034
      });

      setPredictions([
        { service: 'payment-service', predicted_load: '+40%', confidence: 0.87 },
        { service: 'user-service', predicted_load: '+25%', confidence: 0.92 },
        { service: 'inventory-service', predicted_load: '-10%', confidence: 0.78 }
      ]);

    } catch (error) {
      console.error('Failed to fetch ML data:', error);
      setError(error.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchMLData();
    const interval = setInterval(fetchMLData, 30000); // Update every 30s
    return () => clearInterval(interval);
  }, [fetchMLData]);

  // Mock data for charts
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

  if (loading) return <Box display="flex" justifyContent="center" p={4}><CircularProgress /></Box>;

  return (
    <Grid container spacing={3}>
      {error && (
        <Grid item xs={12}>
          <Alert severity="warning">
            ML API connection issues: {error}. Showing cached data.
          </Alert>
        </Grid>
      )}

      {/* ML Model Performance */}
      <Grid item xs={12} md={6}>
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6" gutterBottom>
            🧠 ML Model Performance
          </Typography>
          <Grid container spacing={2}>
            <Grid item xs={4}>
              <Box textAlign="center">
                <Typography variant="h4" color="success.main">
                  {((insights.model_accuracy || 0.952) * 100).toFixed(1)}%
                </Typography>
                <Typography variant="body2">Detection Accuracy</Typography>
              </Box>
            </Grid>
            <Grid item xs={4}>
              <Box textAlign="center">
                <Typography variant="h4" color="info.main">
                  {((1 - (insights.false_positive_rate || 0.034)) * 100).toFixed(1)}%
                </Typography>
                <Typography variant="body2">Precision Rate</Typography>
              </Box>
            </Grid>
            <Grid item xs={4}>
              <Box textAlign="center">
                <Typography variant="h4" color="warning.main">
                  {insights.detection_latency || 12}ms
                </Typography>
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
            {predictions.map((pred, index) => (
              <Alert 
                key={index}
                severity={
                  pred.predicted_load.includes('+40%') ? 'warning' :
                  pred.predicted_load.includes('+') ? 'info' : 'success'
                } 
                sx={{ mb: 1 }}
              >
                {pred.service}: {pred.predicted_load} load expected (confidence: {(pred.confidence * 100).toFixed(0)}%)
              </Alert>
            ))}
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
                {anomalies.map((anomaly, index) => (
                  <TableRow key={anomaly.id || index}>
                    <TableCell>
                      {new Date(anomaly.timestamp).toLocaleString()}
                    </TableCell>
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
                    <TableCell sx={{ maxWidth: 300 }}>
                      <Typography variant="body2" noWrap>
                        {anomaly.description}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography 
                        color={anomaly.score > 0.8 ? 'error.main' : 'warning.main'}
                        fontWeight="bold"
                      >
                        {parseFloat(anomaly.score).toFixed(2)}
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
                {anomalies.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={6}>
                      <Alert severity="info">
                        No recent anomalies detected. System operating normally.
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

// Main App Component with Error Boundary
const App = () => {
  const [currentTab, setCurrentTab] = useState(0);
  const [testDataRunning, setTestDataRunning] = useState(false);
  const [appError, setAppError] = useState(null);

  const handleTabChange = (event, newValue) => {
    setCurrentTab(newValue);
  };

  const generateTestData = async () => {
    setTestDataRunning(true);
    setAppError(null);
    
    try {
      const response = await fetch(`${API_BASE_URL}/logs/generate-test-data`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          count: 5000, 
          rate: 200 
        }),
        timeout: 30000
      });
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      const data = await response.json();
      alert(`✅ Test data generation started! ${data.count} logs at ${data.rate}/sec (ETA: ${data.estimated_duration})`);
      
    } catch (error) {
      console.error('Test data generation failed:', error);
      setAppError(`Failed to generate test data: ${error.message}`);
      alert(`❌ Failed to generate test data: ${error.message}`);
    } finally {
      setTimeout(() => setTestDataRunning(false), 25000); // 25 second generation
    }
  };

  const TabPanel = ({ children, value, index }) => (
    <Box hidden={value !== index} sx={{ pt: 3 }}>
      {value === index && (
        <Box>
          {children}
        </Box>
      )}
    </Box>
  );

  // Error boundary component
  const ErrorBoundary = ({ children }) => {
    if (appError) {
      return (
        <Alert 
          severity="error" 
          action={
            <Button color="inherit" size="small" onClick={() => setAppError(null)}>
              Dismiss
            </Button>
          }
          sx={{ m: 2 }}
        >
          Application Error: {appError}
        </Alert>
      );
    }
    return children;
  };

  return (
    <ErrorBoundary>
      <Box sx={{ flexGrow: 1, minHeight: '100vh', backgroundColor: '#f5f5f5' }}>
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
              startIcon={testDataRunning ? <CircularProgress size={16} color="inherit" /> : <PlayArrowIcon />}
              variant="outlined"
              sx={{ borderColor: 'rgba(255,255,255,0.5)' }}
            >
              {testDataRunning ? 'Generating...' : 'Generate Test Data'}
            </Button>
          </Toolbar>
        </AppBar>

        <Container maxWidth="xl" sx={{ mt: 2, mb: 4 }}>
          {appError && (
            <Alert 
              severity="error" 
              sx={{ mb: 2 }}
              action={
                <Button color="inherit" size="small" onClick={() => setAppError(null)}>
                  Dismiss
                </Button>
              }
            >
              {appError}
            </Alert>
          )}

          <Tabs 
            value={currentTab} 
            onChange={handleTabChange}
            sx={{ 
              borderBottom: 1, 
              borderColor: 'divider', 
              mb: 2,
              '& .MuiTab-root': {
                minWidth: 120
              }
            }}
            variant="scrollable"
            scrollButtons="auto"
          >
            <Tab 
              label="Live Stream" 
              icon={
                <Badge badgeContent="LIVE" color="error">
                  <StorageIcon />
                </Badge>
              }
            />
            <Tab label="Kafka Monitor" icon={<SpeedIcon />} />
            <Tab label="Spark Jobs" icon={<ShowChartIcon />} />
            <Tab label="ML Insights" icon={<PsychologyIcon />} />
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

        {/* Footer */}
        <Box 
          component="footer" 
          sx={{ 
            mt: 'auto', 
            py: 2, 
            px: 3, 
            backgroundColor: '#f5f5f5',
            borderTop: 1,
            borderColor: 'divider'
          }}
        >
          <Typography variant="body2" color="textSecondary" align="center">
            AI Log Analytics Platform | Real-time Streaming with Kafka + Spark + ML
          </Typography>
        </Box>
      </Box>
    </ErrorBoundary>
  );
};

export default App;