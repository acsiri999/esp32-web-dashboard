const MQTT_URL = "wss://broker.emqx.io:8084/mqtt";
const MQTT_TOPICS = ["first_board_backup", "two_board_backup"];
const ONE_HOUR_MS = 60 * 60 * 1000;
const MAX_CHART_POINTS = 1800;
const DEVICE_OFFLINE_TIMEOUT = 20000;
const SENSOR_STALE_TIMEOUT = 60000;

const SENSOR_CONFIG = {
  temperature: { label: "温度", unit: "℃", device: "first", decimals: 2, chart: "tempHumidityChart", physicalMin: -20, physicalMax: 80 },
  humidity: { label: "湿度", unit: "%RH", device: "first", decimals: 2, chart: "tempHumidityChart", physicalMin: 0, physicalMax: 100 },
  pm1_0: { label: "PM1.0", unit: "μg/m³", device: "first", decimals: 0, chart: "particleChart", physicalMin: 0, physicalMax: 1000 },
  pm2_5: { label: "PM2.5", unit: "μg/m³", device: "first", decimals: 0, chart: "particleChart", physicalMin: 0, physicalMax: 1000 },
  pm10: { label: "PM10", unit: "μg/m³", device: "first", decimals: 0, chart: "particleChart", physicalMin: 0, physicalMax: 1500 },
  noise: { label: "噪音", unit: "dB", device: "first", decimals: 1, chart: "noiseChart", physicalMin: 0, physicalMax: 140 },
  co2: { label: "CO₂", unit: "ppm", device: "first", decimals: 0, chart: "co2Eco2Chart", physicalMin: 300, physicalMax: 10000 },
  so2: { label: "SO₂", unit: "ppm", device: "first", decimals: 3, chart: "gasChart", physicalMin: 0, physicalMax: 20 },
  tvoc: { label: "TVOC", unit: "ppb", device: "second", decimals: 0, chart: "gasChart", physicalMin: 0, physicalMax: 10000 },
  eco2: { label: "eCO₂", unit: "ppm", device: "second", decimals: 0, chart: "co2Eco2Chart", physicalMin: 300, physicalMax: 10000 },
  hcho_ppm: { label: "甲醛", unit: "ppm", device: "second", decimals: 3, chart: "gasChart", physicalMin: 0, physicalMax: 5 },
  nh3: { label: "NH₃", unit: "ppm", device: "second", decimals: 3, chart: "gasChart", physicalMin: 0, physicalMax: 200 },
  radon_4h_bq_m3: { label: "氡气4小时均值", unit: "Bq/m³", device: "second", decimals: "auto1", chart: "radonChart", physicalMin: 0, physicalMax: 10000 },
  radon_24h_bq_m3: { label: "氡气24小时均值", unit: "Bq/m³", device: "second", decimals: "auto1", chart: "radonChart", physicalMin: 0, physicalMax: 10000 }
};

const SENSOR_RULES = {
  temperature: { field: "temperature", label: "温度", unit: "℃", weight: 1, enabled: true, type: "range", normalMin: 18, normalMax: 30, warningMin: 10, warningMax: 35, dangerMin: 0, dangerMax: 45 },
  humidity: { field: "humidity", label: "湿度", unit: "%RH", weight: 1, enabled: true, type: "range", normalMin: 35, normalMax: 75, warningMin: 20, warningMax: 85, dangerMin: 10, dangerMax: 95 },
  pm2_5: { field: "pm2_5", label: "PM2.5", unit: "μg/m³", weight: 1.3, enabled: true, type: "max", normalMax: 35, warningMax: 75, dangerMax: 150 },
  pm10: { field: "pm10", label: "PM10", unit: "μg/m³", weight: 1, enabled: true, type: "max", normalMax: 50, warningMax: 150, dangerMax: 250 },
  co2: { field: "co2", label: "CO₂", unit: "ppm", weight: 1.4, enabled: true, type: "max", normalMax: 1000, warningMax: 1500, dangerMax: 2500 },
  eco2: { field: "eco2", label: "eCO₂", unit: "ppm", weight: 0.8, enabled: true, type: "max", normalMax: 1000, warningMax: 1500, dangerMax: 2500 },
  noise: { field: "noise", label: "噪音", unit: "dB", weight: 1, enabled: true, type: "max", normalMax: 70, warningMax: 85, dangerMax: 100 },
  hcho_ppm: { field: "hcho_ppm", label: "甲醛", unit: "ppm", weight: 1.2, enabled: true, type: "max", normalMax: 0.08, warningMax: 0.1, dangerMax: 0.3 },
  radon_24h_bq_m3: { field: "radon_24h_bq_m3", label: "氡气24小时均值", unit: "Bq/m³", weight: 1, enabled: true, type: "max", normalMax: 100, warningMax: 300, dangerMax: 600 },
  radon_4h_bq_m3: { field: "radon_4h_bq_m3", label: "氡气4小时均值", unit: "Bq/m³", weight: 0.7, enabled: true, type: "max", normalMax: 100, warningMax: 300, dangerMax: 600 },
  so2: { field: "so2", label: "SO₂", unit: "ppm", weight: 0, enabled: false, type: "max" },
  tvoc: { field: "tvoc", label: "TVOC", unit: "ppb", weight: 0, enabled: false, type: "max" },
  nh3: { field: "nh3", label: "NH₃", unit: "ppm", weight: 0, enabled: false, type: "max" }
};

const QUALITY_CONFIG = {
  dimensions: [
    { id: "thermal", label: "温湿度环境", fields: ["temperature", "humidity"] },
    { id: "particle", label: "颗粒物环境", fields: ["pm2_5", "pm10"] },
    { id: "freshness", label: "空气新鲜度", fields: ["co2", "eco2"], preferFirstAvailable: true },
    { id: "gas", label: "气体污染物", fields: ["hcho_ppm", "so2", "tvoc", "nh3"] },
    { id: "noise", label: "噪声环境", fields: ["noise"] },
    { id: "radon", label: "氡气环境", fields: ["radon_24h_bq_m3", "radon_4h_bq_m3"], preferFirstAvailable: true }
  ]
};

const topicFieldMap = {
  first_board_backup: {
    device: "first",
    fields: {
      temperature: ["temperature"],
      humidity: ["humidity"],
      pm1_0: ["pm1_0"],
      pm2_5: ["pm2_5"],
      pm10: ["pm10"],
      noise: ["noise_db", "noise_dB", "noise"],
      co2: ["co2_ppm", "co2"],
      so2: ["so2_ppm", "so2"]
    }
  },
  two_board_backup: {
    device: "second",
    fields: {
      tvoc: ["tvoc_ppb"],
      eco2: ["eco2_ppm"],
      hcho_ppm: ["hcho_ppm"],
      nh3: ["nh3_ppm"],
      radon_4h_bq_m3: ["radon_4h_bq_m3"],
      radon_24h_bq_m3: ["radon_24h_bq_m3"]
    }
  }
};

const state = {
  latestData: Object.fromEntries(Object.keys(SENSOR_CONFIG).map((key) => [key, null])),
  devices: {
    first: { timer: null, lastUpdate: null, online: false },
    second: { timer: null, lastUpdate: null, online: false }
  },
  lastDataUpdate: null
};

const sensorStates = Object.fromEntries(Object.keys(SENSOR_CONFIG).map((key) => [key, {
  lastValue: null,
  lastUpdate: null
}]));

const elements = {
  mqttStatus: document.getElementById("mqttStatus"),
  mqttOverview: document.getElementById("mqttOverview"),
  topicText: document.getElementById("topicText"),
  firstBoardStatus: document.getElementById("firstBoardStatus"),
  firstSectionStatus: document.getElementById("firstSectionStatus"),
  firstBoardUpdate: document.getElementById("firstBoardUpdate"),
  firstDeviceSection: document.getElementById("firstDeviceSection"),
  secondBoardStatus: document.getElementById("secondBoardStatus"),
  secondSectionStatus: document.getElementById("secondSectionStatus"),
  secondBoardUpdate: document.getElementById("secondBoardUpdate"),
  secondDeviceSection: document.getElementById("secondDeviceSection"),
  currentDate: document.getElementById("currentDate"),
  currentTime: document.getElementById("currentTime"),
  globalLastUpdate: document.getElementById("globalLastUpdate"),
  onlineDeviceCount: document.getElementById("onlineDeviceCount"),
  metricCount: document.getElementById("metricCount"),
  lastDataUpdate: document.getElementById("lastDataUpdate"),
  qualityScore: document.getElementById("qualityScore"),
  qualityLevel: document.getElementById("qualityLevel"),
  qualityDimensionCount: document.getElementById("qualityDimensionCount"),
  qualityCoverage: document.getElementById("qualityCoverage"),
  dimensionScores: document.getElementById("dimensionScores"),
  riskFactors: document.getElementById("riskFactors")
};

const chartPointElements = {
  tempHumidityChart: document.getElementById("tempHumidityPoints"),
  particleChart: document.getElementById("particlePoints"),
  co2Eco2Chart: document.getElementById("co2Eco2Points"),
  noiseChart: document.getElementById("noisePoints"),
  gasChart: document.getElementById("gasPoints"),
  radonChart: document.getElementById("radonPoints")
};

const chartEmptyElements = {
  tempHumidityChart: document.getElementById("tempHumidityEmpty"),
  particleChart: document.getElementById("particleEmpty"),
  co2Eco2Chart: document.getElementById("co2Eco2Empty"),
  noiseChart: document.getElementById("noiseEmpty"),
  gasChart: document.getElementById("gasEmpty"),
  radonChart: document.getElementById("radonEmpty")
};

elements.topicText.textContent = MQTT_TOPICS.join(" / ");
elements.metricCount.textContent = String(Object.keys(SENSOR_CONFIG).length);

const charts = createCharts();
connectMqtt();
startClock();
updateOverview();
renderQualityPanel();
updateChartPointCounts();

function connectMqtt() {
  setMqttStatus("连接中", "connecting");
  const client = mqtt.connect(MQTT_URL, {
    clientId: `web_dashboard_${Math.random().toString(16).slice(2)}_${Date.now()}`,
    clean: true,
    connectTimeout: 8000,
    reconnectPeriod: 3000,
    keepalive: 60
  });

  client.on("connect", () => {
    console.log("MQTT连接成功");
    setMqttStatus("已连接", "online");
    client.subscribe(MQTT_TOPICS, (error, granted) => {
      if (error) {
        console.error("Topic订阅失败", error);
        setMqttStatus("订阅失败", "offline");
        return;
      }
      console.log("Topic订阅成功", granted.map((item) => item.topic).join("、"));
    });
  });
  client.on("reconnect", () => setMqttStatus("重连中", "connecting"));
  client.on("close", () => setMqttStatus("已断开", "offline"));
  client.on("offline", () => setMqttStatus("离线", "offline"));
  client.on("error", (error) => console.error("MQTT连接错误", error));
  client.on("message", (topic, message) => {
    const rawMessage = message.toString();
    console.log("收到的Topic", topic);
    console.log("收到的原始JSON", rawMessage);
    handleIncomingData(topic, rawMessage);
  });
}

function handleIncomingData(topic, rawMessage) {
  let parsed;
  try {
    parsed = JSON.parse(rawMessage);
    console.log("JSON解析结果", parsed);
  } catch (error) {
    console.error("JSON解析错误", error);
    return;
  }

  if (topic === "first_board_backup") {
    processFirstBoardData(parsed);
  } else if (topic === "two_board_backup") {
    processSecondBoardData(parsed);
  }
}

function processFirstBoardData(data) {
  processBoardData("first_board_backup", data);
}

function processSecondBoardData(data) {
  processBoardData("two_board_backup", data);
}

function processBoardData(topic, data) {
  const topicConfig = topicFieldMap[topic];
  const now = new Date();
  const validUpdates = {};

  Object.entries(topicConfig.fields).forEach(([targetKey, sourceKeys]) => {
    const sourceKey = sourceKeys.find((key) => Object.prototype.hasOwnProperty.call(data, key));
    if (!sourceKey) return;

    const rawValue = data[sourceKey];
    const value = Number(rawValue);
    const isValid = validateSensorValue(targetKey, value);

    if (!isValid) {
      console.warn(`字段 ${sourceKey} 的数值无效，已忽略`, rawValue);
      return;
    }

    updateSensorState(targetKey, value, now);
    state.latestData[targetKey] = value;
    validUpdates[targetKey] = value;
  });

  setDeviceOnline(topicConfig.device, now);
  renderQualityPanel();

  if (Object.keys(validUpdates).length === 0) return;

  state.lastDataUpdate = now;
  updateCards(validUpdates);
  updateCharts(validUpdates, now);
  updateOverview();
}

function validateSensorValue(key, value) {
  const config = SENSOR_CONFIG[key];
  if (!Number.isFinite(value)) return false;
  if (value < config.physicalMin || value > config.physicalMax) {
    return false;
  }
  return true;
}

function updateSensorState(key, value, now) {
  const sensor = sensorStates[key];
  sensor.lastValue = value;
  sensor.lastUpdate = now;
}

function calculateSensorScore(rule, value) {
  if (!rule.enabled || value === null || value === undefined) return null;
  if (rule.type === "range") {
    if (value >= rule.normalMin && value <= rule.normalMax) return 100;
    if (value >= rule.warningMin && value <= rule.warningMax) return 75;
    if (value >= rule.dangerMin && value <= rule.dangerMax) return 45;
    return 20;
  }
  if (value <= rule.normalMax) return 100;
  if (value <= rule.warningMax) return 75;
  if (value <= rule.dangerMax) return 45;
  return 20;
}

function calculateDimensionScore(dimension) {
  const candidates = [];
  for (const field of dimension.fields) {
    const rule = SENSOR_RULES[field];
    const sensor = sensorStates[field];
    const value = state.latestData[field];
    if (!rule || !rule.enabled || value === null || !sensor.lastUpdate) continue;
    if (Date.now() - sensor.lastUpdate.getTime() > SENSOR_STALE_TIMEOUT) continue;
    const score = calculateSensorScore(rule, value);
    if (score !== null) candidates.push({ field, score, weight: rule.weight, label: rule.label });
    if (dimension.preferFirstAvailable && candidates.length) break;
  }

  if (!candidates.length) return { ...dimension, score: null, status: "未配置或无有效数据", impact: "数据不足" };
  const weighted = candidates.reduce((sum, item) => sum + item.score * item.weight, 0);
  const weight = candidates.reduce((sum, item) => sum + item.weight, 0);
  const score = Math.round(weighted / weight);
  return { ...dimension, score, status: getQualityLevel(score).label, impact: candidates.map((item) => item.label).join("、") };
}

function calculateOverallQuality() {
  const dimensions = QUALITY_CONFIG.dimensions.map(calculateDimensionScore);
  const valid = dimensions.filter((item) => item.score !== null);
  const score = valid.length ? Math.round(valid.reduce((sum, item) => sum + item.score, 0) / valid.length) : null;
  return {
    score,
    level: getQualityLevel(score),
    dimensions,
    validCount: valid.length,
    coverage: Math.round((valid.length / dimensions.length) * 100)
  };
}

function getQualityLevel(score) {
  if (score === null) return { label: "等待数据", className: "level-none", color: "#6f849e" };
  if (score >= 90) return { label: "优秀", className: "level-excellent", color: "#28d7a1" };
  if (score >= 75) return { label: "良好", className: "level-good", color: "#2f8cff" };
  if (score >= 60) return { label: "一般", className: "level-normal", color: "#ffdf6b" };
  if (score >= 40) return { label: "较差", className: "level-poor", color: "#ffb547" };
  return { label: "危险", className: "level-danger", color: "#ff5d6c" };
}

function generateRiskFactors(quality) {
  const risks = quality.dimensions
    .filter((item) => item.score !== null)
    .sort((a, b) => a.score - b.score)
    .slice(0, 3)
    .filter((item) => item.score < 75)
    .map((item) => ({
      title: `${item.label}评分偏低`,
      detail: `主要影响参数：${item.impact}`,
      advice: getAdvice(item.id)
    }));

  if (quality.coverage < 100) {
    risks.push({ title: "数据覆盖不完整", detail: `当前覆盖率 ${quality.coverage}%`, advice: "建议检查传感器连接状态与上传字段完整性" });
  }

  return risks.length ? risks : [{ title: "当前监测参数未发现明显异常", detail: "综合评分处于可接受范围", advice: "继续保持实时监测" }];
}

function getAdvice(id) {
  const map = {
    thermal: "建议检查现场遮阳、通风或喷淋降温措施",
    particle: "建议检查扬尘控制措施并加强洒水降尘",
    freshness: "建议加强现场通风，降低人员密集区域 CO₂ 浓度",
    gas: "建议检查气体来源并确认传感器连接状态",
    noise: "建议优化施工时段或增加降噪隔离措施",
    radon: "建议复核氡气传感器位置并保持持续通风"
  };
  return map[id] || "建议持续观察";
}

function renderQualityPanel() {
  const quality = calculateOverallQuality();
  const score = quality.score === null ? "--" : String(quality.score);
  elements.qualityScore.textContent = score;
  elements.qualityLevel.textContent = quality.level.label;
  elements.qualityLevel.className = `level-badge ${quality.level.className}`;
  elements.qualityDimensionCount.textContent = String(quality.validCount);
  elements.qualityCoverage.textContent = `${quality.coverage}%`;

  const ring = document.querySelector(".score-ring");
  ring.style.setProperty("--score-deg", `${quality.score === null ? 0 : quality.score * 3.6}deg`);
  ring.style.setProperty("--score-color", quality.level.color);

  elements.dimensionScores.innerHTML = quality.dimensions.map((item) => `
    <div class="dimension-item">
      <div><strong>${item.label}</strong><span>${item.impact}</span></div>
      <em>${item.score === null ? "--" : item.score}</em>
      <div class="progress"><i style="width:${item.score || 0}%"></i></div>
      <small>${item.status}</small>
    </div>
  `).join("");

  elements.riskFactors.innerHTML = generateRiskFactors(quality).map((risk) => `
    <div class="risk-item"><strong>${risk.title}</strong><span>${risk.detail}</span><p>${risk.advice}</p></div>
  `).join("");
}

function updateCards(updates) {
  Object.entries(updates).forEach(([key, value]) => {
    const valueElement = document.getElementById(`${key}Value`);
    const config = SENSOR_CONFIG[key];
    if (valueElement && config) {
      valueElement.textContent = formatSensorNumber(value, config.decimals);
      restartClassAnimation(valueElement, "value-updated");
    }
  });
  updateAlarm("co2", state.latestData.co2, [{ level: "danger", value: 1500, text: "报警" }, { level: "warning", value: 1000, text: "预警" }]);
  updateAlarm("noise", state.latestData.noise, [{ level: "danger", value: 85, text: "报警" }, { level: "warning", value: 70, text: "预警" }]);
  updateAlarm("hcho_ppm", state.latestData.hcho_ppm, [{ level: "danger", value: 0.1, text: "报警" }, { level: "warning", value: 0.08, text: "预警" }]);
}

function updateAlarm(key, value, rules) {
  const alarmElement = document.getElementById(`${key}Alarm`);
  const cardElement = document.querySelector(`.data-card[data-key="${key}"]`);
  let result = { level: "normal", text: "正常" };
  if (Number.isFinite(value)) result = rules.find((rule) => value >= rule.value) || result;
  if (alarmElement) {
    alarmElement.textContent = result.text;
    alarmElement.className = `alarm ${result.level}`;
  }
  if (cardElement) {
    cardElement.classList.remove("normal", "warning", "danger");
    cardElement.classList.add(result.level);
  }
}

function updateCharts(updates, date) {
  const timestamp = date.getTime();
  Object.entries(updates).forEach(([key, value]) => {
    const chart = charts[SENSOR_CONFIG[key].chart];
    if (!chart) return;
    const dataset = chart.data.datasets.find((item) => item.key === key);
    if (dataset) dataset.data.push({ x: timestamp, y: value });
  });
  pruneAllCharts(timestamp);
  updateChartWindows(timestamp);
  updateChartPointCounts();
  Object.values(charts).forEach((chart) => chart.update("none"));
}

function pruneAllCharts(nowTimestamp) {
  const cutoff = nowTimestamp - ONE_HOUR_MS;
  Object.values(charts).forEach((chart) => {
    chart.data.datasets.forEach((dataset) => {
      dataset.data = compactDataPoints(dataset.data.filter((point) => point.x >= cutoff));
    });
  });
}

function compactDataPoints(points) {
  if (points.length <= MAX_CHART_POINTS) return points;
  const compacted = [];
  const step = (points.length - 1) / (MAX_CHART_POINTS - 1);
  for (let index = 0; index < MAX_CHART_POINTS; index += 1) compacted.push(points[Math.round(index * step)]);
  return compacted;
}

function updateChartWindows(nowTimestamp) {
  const earliestTimestamp = getEarliestChartTimestamp();
  const minTimestamp = earliestTimestamp ? Math.max(earliestTimestamp, nowTimestamp - ONE_HOUR_MS) : nowTimestamp - ONE_HOUR_MS;
  Object.values(charts).forEach((chart) => {
    chart.options.scales.x.min = minTimestamp;
    chart.options.scales.x.max = nowTimestamp;
  });
}

function getEarliestChartTimestamp() {
  let earliest = null;
  Object.values(charts).forEach((chart) => {
    chart.data.datasets.forEach((dataset) => {
      dataset.data.forEach((point) => {
        if (earliest === null || point.x < earliest) earliest = point.x;
      });
    });
  });
  return earliest;
}

function updateChartPointCounts() {
  Object.entries(charts).forEach(([chartId, chart]) => {
    const count = chart.data.datasets.reduce((total, dataset) => total + dataset.data.length, 0);
    if (chartPointElements[chartId]) chartPointElements[chartId].textContent = `${count}点`;
    if (chartEmptyElements[chartId]) chartEmptyElements[chartId].classList.toggle("hidden", count > 0);
  });
}

function createCharts() {
  return {
    tempHumidityChart: createLineChart("tempHumidityChart", [["temperature", "温度", "#2f8cff"], ["humidity", "湿度", "#21d4d8"]], "数值"),
    particleChart: createLineChart("particleChart", [["pm1_0", "PM1.0", "#21d4d8"], ["pm2_5", "PM2.5", "#7a6cff"], ["pm10", "PM10", "#2f8cff"]], "颗粒物浓度（μg/m³）"),
    co2Eco2Chart: createLineChart("co2Eco2Chart", [["co2", "CO₂", "#28d7a1"], ["eco2", "eCO₂", "#ffb547"]], "浓度（ppm）"),
    noiseChart: createLineChart("noiseChart", [["noise", "噪音", "#ff5d6c"]], "噪音（dB）"),
    gasChart: createLineChart("gasChart", [["so2", "SO₂", "#7a6cff"], ["tvoc", "TVOC", "#21d4d8"], ["hcho_ppm", "甲醛", "#ffb547"], ["nh3", "NH₃", "#28d7a1"]], "污染物浓度"),
    radonChart: createLineChart("radonChart", [["radon_4h_bq_m3", "氡气4小时均值", "#7a6cff"], ["radon_24h_bq_m3", "氡气24小时均值", "#2f8cff"]], "氡气浓度（Bq/m³）")
  };
}

function createLineChart(canvasId, datasets, yTitle) {
  return new Chart(document.getElementById(canvasId), {
    type: "line",
    data: { datasets: datasets.map(([key, label, color]) => createDataset(key, label, color, hexToRgba(color, 0.08))) },
    options: baseChartOptions(yTitle)
  });
}

function hexToRgba(hex, alpha) {
  const value = hex.replace("#", "");
  const red = parseInt(value.slice(0, 2), 16);
  const green = parseInt(value.slice(2, 4), 16);
  const blue = parseInt(value.slice(4, 6), 16);
  return `rgba(${red}, ${green}, ${blue}, ${alpha})`;
}

function createDataset(key, label, color, fillColor) {
  return {
    key,
    label,
    data: [],
    borderColor: color,
    backgroundColor: fillColor,
    borderWidth: 2,
    pointRadius(context) { return context.dataset.data.length > 120 ? 0 : 1.8; },
    pointHoverRadius: 5,
    pointBackgroundColor: color,
    pointBorderColor: "#071321",
    pointBorderWidth: 1,
    tension: 0.3,
    parsing: false,
    fill: true
  };
}

function baseChartOptions(yTitle) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    normalized: true,
    interaction: { mode: "nearest", intersect: false },
    plugins: {
      legend: { position: "top", align: "start", labels: { usePointStyle: true, boxWidth: 8, color: "#b8c7dc", padding: 14, font: { family: '"Microsoft YaHei", "Segoe UI", Arial, sans-serif' } } },
      tooltip: {
        backgroundColor: "rgba(6, 16, 31, 0.94)",
        titleColor: "#f4f8ff",
        bodyColor: "#c8d5e6",
        borderColor: "rgba(47, 140, 255, 0.45)",
        borderWidth: 1,
        cornerRadius: 10,
        callbacks: {
          title(items) { return items.length ? formatTime(new Date(items[0].parsed.x)) : ""; },
          label(item) {
            const unit = SENSOR_CONFIG[item.dataset.key] ? SENSOR_CONFIG[item.dataset.key].unit : "";
            return `${item.dataset.label}: ${item.parsed.y} ${unit}`;
          }
        }
      }
    },
    scales: {
      x: { type: "linear", border: { color: "rgba(130, 185, 255, 0.12)" }, ticks: { maxRotation: 0, autoSkip: true, color: "#91a8c4", callback(value) { return formatTime(new Date(value)); } }, grid: { color: "rgba(130, 185, 255, 0.07)" } },
      y: { beginAtZero: false, border: { color: "rgba(130, 185, 255, 0.12)" }, title: { display: true, text: yTitle, color: "#9fb2c9" }, ticks: { color: "#91a8c4" }, grid: { color: "rgba(130, 185, 255, 0.07)" } }
    }
  };
}

function setMqttStatus(text, status) {
  updateStatusPill(elements.mqttStatus, `MQTT ${text}`, status);
  elements.mqttOverview.textContent = text;
  updateOverview();
}

function setDeviceOnline(deviceKey, date) {
  const device = state.devices[deviceKey];
  const name = deviceKey === "first" ? "第一块板" : "第二块板";
  const headerStatus = deviceKey === "first" ? elements.firstBoardStatus : elements.secondBoardStatus;
  const sectionStatus = deviceKey === "first" ? elements.firstSectionStatus : elements.secondSectionStatus;
  const updateElement = deviceKey === "first" ? elements.firstBoardUpdate : elements.secondBoardUpdate;
  const sectionElement = deviceKey === "first" ? elements.firstDeviceSection : elements.secondDeviceSection;
  device.online = true;
  device.lastUpdate = date;
  updateStatusPill(headerStatus, `${name} 在线`, "online");
  updateStatusPill(sectionStatus, "在线", "online");
  updateElement.textContent = `最后接收：${formatDateTime(date)}`;
  sectionElement.classList.remove("device-offline");
  clearTimeout(device.timer);
  device.timer = setTimeout(() => setDeviceOffline(deviceKey), DEVICE_OFFLINE_TIMEOUT);
  updateOverview();
}

function setDeviceOffline(deviceKey) {
  const device = state.devices[deviceKey];
  const name = deviceKey === "first" ? "第一块板" : "第二块板";
  const headerStatus = deviceKey === "first" ? elements.firstBoardStatus : elements.secondBoardStatus;
  const sectionStatus = deviceKey === "first" ? elements.firstSectionStatus : elements.secondSectionStatus;
  const sectionElement = deviceKey === "first" ? elements.firstDeviceSection : elements.secondDeviceSection;
  device.online = false;
  updateStatusPill(headerStatus, `${name} 离线`, "offline");
  updateStatusPill(sectionStatus, "离线", "offline");
  sectionElement.classList.add("device-offline");
  updateOverview();
  renderQualityPanel();
}

function updateStatusPill(element, text, status) {
  element.textContent = text;
  element.className = `status-pill status-${status}`;
}

function updateOverview() {
  const onlineCount = Number(state.devices.first.online) + Number(state.devices.second.online);
  elements.onlineDeviceCount.textContent = String(onlineCount);
  const updateText = state.lastDataUpdate ? formatDateTime(state.lastDataUpdate) : "--";
  elements.lastDataUpdate.textContent = updateText;
  elements.globalLastUpdate.textContent = `最后更新 ${updateText}`;
}

function startClock() {
  updateClock();
  setInterval(() => {
    updateClock();
    runLightweightStatusCheck();
  }, 1000);
}

function runLightweightStatusCheck() {
  renderQualityPanel();
}

function updateClock() {
  const now = new Date();
  elements.currentDate.textContent = now.toLocaleDateString("zh-CN");
  elements.currentTime.textContent = formatTime(now);
}

function restartClassAnimation(element, className) {
  element.classList.remove(className);
  void element.offsetWidth;
  element.classList.add(className);
}

function formatSensorNumber(value, decimals) {
  if (decimals === "auto1") return Number.isInteger(value) ? String(value) : value.toFixed(1);
  return Number(value).toFixed(decimals);
}

function formatTime(date) {
  return date.toLocaleTimeString("zh-CN", { hour12: false });
}

function formatDateTime(date) {
  return date.toLocaleString("zh-CN", { hour12: false });
}
