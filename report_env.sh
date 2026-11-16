#!/usr/bin/env bash
# Run from ~/Workflow: bash env_report.sh  -> writes env_report.txt
cd "$(dirname "$0")" || exit 1
{
echo "== SYSTEM"
lscpu | grep -E "Model name|Core\(s\)|Thread|L2|L3|max MHz"
free -h | head -2
lsblk -d -o NAME,MODEL,SIZE
uname -sr
grep PRETTY_NAME /etc/os-release

for v in .venv .venv-t; do
  echo; echo "== PYTHON ($v)"
  $v/bin/python - <<'EOF'
import sys, importlib.metadata as m
print(sys.version)
print("GIL enabled:", getattr(sys, "_is_gil_enabled", lambda: "n/a")())
for d in sorted(m.distributions(), key=lambda d: d.metadata["Name"].lower()):
    print(f"{d.metadata['Name']}=={d.version}")
EOF
done

echo; echo "== JAVA / JYTHON"
java -version 2>&1
find / \( -name "jython*.jar" -o -name "postgresql*.jar" \) -not -path "/proc/*" 2>/dev/null
} > env_report.txt 2>&1
echo "Done: env_report.txt"
