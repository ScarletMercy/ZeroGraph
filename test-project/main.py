"""ZeroGraph 全功能集成测试 — 运行所有场景并输出报告。"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scenarios import s01_basic, s02_state, s03_checkpoint, s04_interrupt
from scenarios import s05_send, s06_subgraph, s07_streaming, s08_advanced
from scenarios import s09_agents, s10_async, s11_funcapi

SCENARIOS = [
    ("01-基础图构建", s01_basic),
    ("02-状态管理", s02_state),
    ("03-检查点持久化", s03_checkpoint),
    ("04-中断与恢复", s04_interrupt),
    ("05-动态路由Send", s05_send),
    ("06-子图嵌套", s06_subgraph),
    ("07-流模式", s07_streaming),
    ("08-高级功能", s08_advanced),
    ("09-多智能体", s09_agents),
    ("10-异步执行", s10_async),
    ("11-函数式API", s11_funcapi),
]


def main():
    total_pass = 0
    total_fail = 0
    start = time.monotonic()

    for name, module in SCENARIOS:
        print(f"\n{'='*60}")
        print(f"  场景: {name}")
        print(f"{'='*60}")
        try:
            results = module.run()
        except Exception as e:
            print(f"  [FATAL] 场景执行异常: {e}")
            import traceback
            traceback.print_exc()
            total_fail += 1
            continue

        passed = sum(1 for _, ok, _ in results if ok)
        failed = len(results) - passed

        for test_name, ok, detail in results:
            status = "\033[92mPASS\033[0m" if ok else "\033[91mFAIL\033[0m"
            print(f"  [{status}] {test_name}: {detail}")

        print(f"  -- {passed} 通过, {failed} 失败 --")
        total_pass += passed
        total_fail += failed

    elapsed = time.monotonic() - start
    print(f"\n{'='*60}")
    print(f"  总计: {total_pass} 通过, {total_fail} 失败 ({elapsed:.2f}s)")
    if total_fail == 0:
        print("  \033[92m所有测试通过！\033[0m")
    else:
        print("  \033[91m存在失败的测试！\033[0m")
    print(f"{'='*60}")
    return 0 if total_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
