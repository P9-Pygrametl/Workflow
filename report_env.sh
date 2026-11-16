{
echo "===== HARDWARE EXTRAS ====="
lscpu | grep -E "Model name|Thread|Core|Socket|L1d|L1i|L2|L3|max MHz|min MHz"
lscpu -e=CPU,CORE,MAXMHZ,MINMHZ | head -30
cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor
cat /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null
sudo dmidecode -t memory | grep -E "Size|Type:|Speed|Configured"

echo; echo "===== OS ====="
uname -a
cat /etc/os-release | head -3
gcc --version | head -1

echo; echo "===== CPYTHON 3.15 (.venv) ====="
.venv/bin/python -VV
.venv/bin/python -c "import sys, sysconfig; print(sys.version); print(sys.flags); print(sysconfig.get_config_var('CONFIG_ARGS')); print('GIL enabled:', getattr(sys, '_is_gil_enabled', lambda: 'n/a')())"
.venv/bin/python -m pip freeze

echo; echo "===== CPYTHON 3.15t (.venv-t) ====="
.venv-t/bin/python -VV
.venv-t/bin/python -c "import sys, sysconfig; print(sys.version); print(sys.flags); print(sysconfig.get_config_var('CONFIG_ARGS')); print('Py_GIL_DISABLED:', sysconfig.get_config_var('Py_GIL_DISABLED')); print('GIL enabled:', sys._is_gil_enabled())"
.venv-t/bin/python -m pip freeze
echo "PYTHON_GIL=$PYTHON_GIL"

echo; echo "===== JAVA / JYTHON ====="
java -version 2>&1
which java; readlink -f "$(which java)"
echo "JAVA_OPTS=$JAVA_OPTS  JYTHON_OPTS=$JYTHON_OPTS  JAVA_HOME=$JAVA_HOME"
ls -la jython
find jython -maxdepth 2 \( -name "jython*" -o -name "registry" \) | head
find jython -maxdepth 3 -path "*bin/jython" -exec {} --version \; 2>&1
find jython -maxdepth 3 -name "*.jar" | head
ls .jython_cache | head
cat requirements.txt

echo; echo "===== POSTGRES / DOCKER ====="
docker --version
docker compose version
cat docker-compose.yml
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"
docker compose images
CID=$(docker compose ps -q | head -1); docker exec "$CID" psql -U postgres -c "select version();" 2>&1
docker exec "$CID" psql -U postgres -c "select name, setting, unit from pg_settings where name in ('shared_buffers','work_mem','effective_cache_size','max_connections','fsync','synchronous_commit','max_parallel_workers_per_gather');" 2>&1
docker stats --no-stream

echo; echo "===== BENCHMARK CODE ====="
git rev-parse HEAD
git status --short
git log -1 --format="%H %cd"
ls benchmarks pipelined run_all shared helpers
grep -v -i -E "pass|secret|token|key" .env

echo; echo "===== DATA ====="
du -sh data
ls data | head
head -5 merged.csv 2>/dev/null
wc -l merged.csv 2>/dev/null
} > env_report.txt 2>&1
