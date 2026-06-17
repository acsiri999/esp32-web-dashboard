const MQTT_URL = "wss://broker.emqx.io:8084/mqtt";
const MQTT_TOPICS = ["first_board_backup", "two_board_backup"];
const ONE_HOUR_MS = 60 * 60 * 1000;
const MAX_CHART_POINTS = 1800;
const DEVICE_OFFLINE_TIMEOUT = 20000;

const state = {
  latestData: {
    temperature: null,
    humidity: null,
    pm1_0: null,
    pm2_5: null,
    pm10: null,
    noise: null,
    co2: null,
    so2: null,
    tvoc: null,
    eco2: null,
    hcho_ppm: null,
    nh3: null,
    radon_4h_bq_m3: null,
    radon_24h_bq_m3: null
  },
  devices: {
    first: { timer: null, lastUpdate: null, online: false },
    second: { timer: null, lastUpdate: null, online: false }
  },
  lastDataUpdate: null
};

const fields = {
  temperature: { decimals: 2, chart: "tempHumidityChart", unit: "℃" },
  humidity: { decimals: 2, chart: "tempHumidityChart", unit: "%RH" },
  pm1_0: { decimals: 0, chart: "particleChart", unit: "μg/m³" },
  pm2_5: { decimals: 0, chart: "particleChart", unit: "μg/m³" },
  pm10: { decimals: 0, chart: "particleChart", unit: "μg/m³" },
  noise: { decimals: 1, chart: "noiseChart", unit: "dB" },
  co2: { decimals: 0, chart: "co2Eco2Chart", unit: "ppm" },
  so2: { decimals: 3, chart: "gasChart", unit: "ppm" },
  tvoc: { decimals: 0, chart: "gasChart", unit: "ppb" },
  eco2: { decimals: 0, chart: "co2Eco2Chart", unit: "ppm" },
  hcho_ppm: { decimals: 3, chart: "gasChart", unit: "ppm" },
  nh3: { decimals: 3, chart: "gasChart", unit: "ppm" },
  radon_4h_bq_m3: { decimals: "auto1", chart: "radonChart", unit: "Bq/m³" },
  radon_24h_bq_m3: { decimals: "auto1", chart: "radonChart", unit: "Bq/m³" }
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
  lastDataUpdate: document.getElementById("lastDataUpdate")
};

elements.topicText.textContent = MQTT_TOPICS.join(" / ");
elements.metricCount.textContent = String(Object.keys(fields).length);

const charts = createCharts();
connectMqtt();
startClock();
updateOverview();
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

      const topics = granted.map((item) => item.topic).join("、");
      console.log("Topic订阅成功", topics);
    });
  });

  client.on("reconnect", () => {
    setMqttStatus("重连中", "connecting");
  });

  client.on("close", () => {
    setMqttStatus("已断开", "offline");
  });

  client.on("offline", () => {
    setMqttStatus("离线", "offline");
  });

  client.on("error", (error) => {
    console.error("MQTT连接错误", error);
  });

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

  processSensorData(topic, parsed);
}

function processSensorData(topic, data) {
  const topicConfig = topicFieldMap[topic];

  if (!topicConfig) {
    return;
  }

  const validUpdates = {};
  const now = new Date();

  Object.entries(topicConfig.fields).forEach(([targetKey, sourceKeys]) => {
    const sourceKey = sourceKeys.find((key) => Object.prototype.hasOwnProperty.call(data, key));

    if (!sourceKey) {
      return;
    }

    const value = Number(data[sourceKey]);
    if (!Number.isFinite(value)) {
      console.warn(`字段 ${sourceKey} 的数值无效，已忽略`, data[sourceKey]);
      return;
    }

    state.latestData[targetKey] = value;
    validUpdates[targetKey] = value;
  });

  setDeviceOnline(topicConfig.device, now);

  if (Object.keys(validUpdates).length === 0) {
    return;
  }

  state.lastDataUpdate = now;
  updateCards(validUpdates);
  updateCharts(validUpdates, now);
  updateOverview();
}

function updateCards(updates) {
  Object.entries(updates).forEach(([key, value]) => {
    const valueElement = document.getElementById(`${key}Value`);
    const config = fields[key];

    if (valueElement && config) {
      valueElement.textContent = formatSensorNumber(value, config.decimals);
      restartClassAnimation(valueElement, "value-updated");
    }
  });

  updateAlarm("co2", state.latestData.co2, [
    { level: "danger", value: 1500, text: "报警" },
    { level: "warning", value: 1000, text: "预警" }
  ]);
  updateAlarm("noise", state.latestData.noise, [
    { level: "danger", value: 85, text: "报警" },
    { level: "warning", value: 70, text: "预警" }
  ]);
  updateAlarm("hcho_ppm", state.latestData.hcho_ppm, [
    { level: "danger", value: 0.1, text: "报警" },
    { level: "warning", value: 0.08, text: "预警" }
  ]);
}

function updateAlarm(key, value, rules) {
  const alarmElement = document.getElementById(`${key}Alarm`);
  const cardElement = document.querySelector(`.data-card[data-key="${key}"]`);
  let result = { level: "normal", text: "正常" };

  if (Number.isFinite(value)) {
    result = rules.find((rule) => value >= rule.value) || result;
  }

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
    const chartId = fields[key].chart;
    const chart = charts[chartId];

    if (!chart) {
      return;
    }

    const dataset = chart.data.datasets.find((item) => item.key === key);
    if (!dataset) {
      return;
    }

    dataset.data.push({ x: timestamp, y: value });
  });

  pruneAllCharts(timestamp);
  updateChartWindows(timestamp);
  updateChartPointCounts();

  Object.values(charts).forEach((chart) => {
    chart.update("none");
  });
}

function pruneAllCharts(nowTimestamp) {
  const cutoff = nowTimestamp - ONE_HOUR_MS;

  Object.values(charts).forEach((chart) => {
    chart.data.datasets.forEach((dataset) => {
      dataset.data = dataset.data.filter((point) => point.x >= cutoff);
      dataset.data = compactDataPoints(dataset.data);
    });
  });
}

function compactDataPoints(points) {
  if (points.length <= MAX_CHART_POINTS) {
    return points;
  }

  const compacted = [];
  const step = (points.length - 1) / (MAX_CHART_POINTS - 1);

  for (let index = 0; index < MAX_CHART_POINTS; index += 1) {
    compacted.push(points[Math.round(index * step)]);
  }

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
        if (earliest === null || point.x < earliest) {
          earliest = point.x;
        }
      });
    });
  });

  return earliest;
}

function updateChartPointCounts() {
  Object.entries(charts).forEach(([chartId, chart]) => {
    const count = chart.data.datasets.reduce((total, dataset) => total + dataset.data.length, 0);
    const countElement = chartPointElements[chartId];
    const emptyElement = chartEmptyElements[chartId];

    if (countElement) {
      countElement.textContent = `${count}点`;
    }

    if (emptyElement) {
      emptyElement.classList.toggle("hidden", count > 0);
    }
  });
}

function createCharts() {
  return {
    tempHumidityChart: new Chart(document.getElementById("tempHumidityChart"), {
      type: "line",
      data: {
        datasets: [
          createDataset("temperature", "温度", "#2f8cff", "rgba(47, 140, 255, 0.10)"),
          createDataset("humidity", "湿度", "#21d4d8", "rgba(33, 212, 216, 0.09)")
        ]
      },
      options: baseChartOptions("数值")
    }),
    particleChart: new Chart(document.getElementById("particleChart"), {
      type: "line",
      data: {
        datasets: [
          createDataset("pm1_0", "PM1.0", "#21d4d8", "rgba(33, 212, 216, 0.08)"),
          createDataset("pm2_5", "PM2.5", "#7a6cff", "rgba(122, 108, 255, 0.08)"),
          createDataset("pm10", "PM10", "#2f8cff", "rgba(47, 140, 255, 0.08)")
        ]
      },
      options: baseChartOptions("颗粒物浓度（μg/m³）")
    }),
    co2Eco2Chart: new Chart(document.getElementById("co2Eco2Chart"), {
      type: "line",
      data: {
        datasets: [
          createDataset("co2", "CO₂", "#28d7a1", "rgba(40, 215, 161, 0.09)"),
          createDataset("eco2", "eCO₂", "#ffb547", "rgba(255, 181, 71, 0.08)")
        ]
      },
      options: baseChartOptions("浓度（ppm）")
    }),
    noiseChart: new Chart(document.getElementById("noiseChart"), {
      type: "line",
      data: {
        datasets: [createDataset("noise", "噪音", "#ff5d6c", "rgba(255, 93, 108, 0.08)")]
      },
      options: baseChartOptions("噪音（dB）")
    }),
    gasChart: new Chart(document.getElementById("gasChart"), {
      type: "line",
      data: {
        datasets: [
          createDataset("so2", "SO₂", "#7a6cff", "rgba(122, 108, 255, 0.08)"),
          createDataset("tvoc", "TVOC", "#21d4d8", "rgba(33, 212, 216, 0.08)"),
          createDataset("hcho_ppm", "甲醛", "#ffb547", "rgba(255, 181, 71, 0.08)"),
          createDataset("nh3", "NH₃", "#28d7a1", "rgba(40, 215, 161, 0.08)")
        ]
      },
      options: baseChartOptions("污染物浓度")
    }),
    radonChart: new Chart(document.getElementById("radonChart"), {
      type: "line",
      data: {
        datasets: [
          createDataset("radon_4h_bq_m3", "氡气4小时均值", "#7a6cff", "rgba(122, 108, 255, 0.09)"),
          createDataset("radon_24h_bq_m3", "氡气24小时均值", "#2f8cff", "rgba(47, 140, 255, 0.09)")
        ]
      },
      options: baseChartOptions("氡气浓度（Bq/m³）")
    })
  };
}

function createDataset(key, label, color, fillColor) {
  return {
    key,
    label,
    data: [],
    borderColor: color,
    backgroundColor: fillColor,
    borderWidth: 2,
    pointRadius(context) {
      return context.dataset.data.length > 120 ? 0 : 1.8;
    },
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
    interaction: {
      mode: "nearest",
      intersect: false
    },
    plugins: {
      legend: {
        position: "top",
        align: "start",
        labels: {
          usePointStyle: true,
          boxWidth: 8,
          color: "#b8c7dc",
          padding: 14,
          font: {
            family: '"Microsoft YaHei", "Segoe UI", Arial, sans-serif'
          }
        }
      },
      tooltip: {
        backgroundColor: "rgba(6, 16, 31, 0.94)",
        titleColor: "#f4f8ff",
        bodyColor: "#c8d5e6",
        borderColor: "rgba(47, 140, 255, 0.45)",
        borderWidth: 1,
        cornerRadius: 10,
        displayColors: true,
        callbacks: {
          title(items) {
            if (!items.length) {
              return "";
            }
            return formatTime(new Date(items[0].parsed.x));
          },
          label(item) {
            const datasetKey = item.dataset.key;
            const unit = fields[datasetKey] ? fields[datasetKey].unit : "";
            return `${item.dataset.label}: ${item.parsed.y} ${unit}`;
          }
        }
      }
    },
    scales: {
      x: {
        type: "linear",
        border: {
          color: "rgba(130, 185, 255, 0.12)"
        },
        ticks: {
          maxRotation: 0,
          autoSkip: true,
          color: "#91a8c4",
          callback(value) {
            return formatTime(new Date(value));
          }
        },
        grid: {
          color: "rgba(130, 185, 255, 0.07)"
        }
      },
      y: {
        beginAtZero: false,
        border: {
          color: "rgba(130, 185, 255, 0.12)"
        },
        title: {
          display: true,
          text: yTitle,
          color: "#9fb2c9",
          font: {
            family: '"Microsoft YaHei", "Segoe UI", Arial, sans-serif'
          }
        },
        ticks: {
          color: "#91a8c4"
        },
        grid: {
          color: "rgba(130, 185, 255, 0.07)"
        }
      }
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
  setInterval(updateClock, 1000);
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
  if (decimals === "auto1") {
    return Number.isInteger(value) ? String(value) : value.toFixed(1);
  }

  return Number(value).toFixed(decimals);
}

function formatTime(date) {
  return date.toLocaleTimeString("zh-CN", { hour12: false });
}

function formatDateTime(date) {
  return date.toLocaleString("zh-CN", { hour12: false });
}
