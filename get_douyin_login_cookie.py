# -*- coding: utf-8 -*-
"""
从浏览器自动获取抖音登录后的Cookie，更新到 crawlers/douyin/web/config.yaml
使用方法：
1. 在浏览器（Chrome/Edge）里打开 https://www.douyin.com 并登录
2. 运行本脚本：python get_douyin_login_cookie.py
3. 脚本会自动从浏览器获取Cookie并更新到config.yaml
"""
import os
import yaml


def get_douyin_cookie():
    """从浏览器获取抖音Cookie（依次尝试Chrome、Edge、Firefox）"""
    import browser_cookie3
    cookie_str = ""

    browsers = [
        ("Chrome", browser_cookie3.chrome),
        ("Edge", browser_cookie3.edge),
        ("Firefox", browser_cookie3.firefox),
    ]

    for name, func in browsers:
        try:
            cj = func(domain_name='douyin.com')
            parts = []
            for cookie in cj:
                parts.append(f"{cookie.name}={cookie.value}")
            if parts:
                cookie_str = "; ".join(parts)
                print(f"✅ 从{name}获取到抖音Cookie（{len(parts)}个字段）")
                return cookie_str
        except Exception as e:
            print(f"⚠️ {name}获取失败：{e}")

    return None


def update_config(cookie_str):
    """更新config.yaml里的Cookie"""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "crawlers", "douyin", "web", "config.yaml")
    if not os.path.isfile(config_path):
        print(f"❌ 配置文件不存在：{config_path}")
        return False

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    cfg["TokenManager"]["douyin"]["headers"]["Cookie"] = cookie_str

    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False, sort_keys=False)

    print(f"✅ Cookie已更新到：{config_path}")

    # 检查是否包含登录态关键字段
    login_fields = ["sessionid", "sessionid_ss", "sid_guard", "uid_tt", "passport_csrf_token"]
    found = [f for f in login_fields if f in cookie_str]
    if found:
        print(f"✅ 检测到登录态字段：{', '.join(found)}，应该能解析更高清晰度")
    else:
        print("⚠️ 未检测到登录态字段，请确认浏览器已登录抖音")

    return True


if __name__ == "__main__":
    print("=" * 60)
    print("抖音登录Cookie自动获取工具")
    print("=" * 60)
    print("请确保已在Chrome/Edge/Firefox里登录 https://www.douyin.com")
    print("=" * 60)

    cookie = get_douyin_cookie()
    if cookie:
        update_config(cookie)
        print("\n✅ 完成！重启qq-bot容器后生效：")
        print("   docker compose -p qqbot restart qqbot")
    else:
        print("\n❌ 未能从浏览器获取到抖音Cookie")
        print("\n手动获取方法：")
        print("1. 浏览器打开 https://www.douyin.com 并登录")
        print("2. 按 F12 打开开发者工具 → Network（网络）标签")
        print("3. 刷新页面，点击任意一个请求")
        print("4. 在 Request Headers 里找到 Cookie 字段，复制完整内容")
        print("5. 粘贴到 crawlers/douyin/web/config.yaml 的 Cookie 字段")
