#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# import
import os
import sys
import csv
import locale
import logging
import base64
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, Union, List
from dotenv import load_dotenv
from urllib.parse import parse_qs, urlparse

# 3rd party imports
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import discord
from discord.ext import commands
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

import dotenv
from server import server_thread

# Load environment variables
BASE_DIR = os.getcwd()
# load_dotenv(os.path.join(BASE_DIR, '.env'))
dotenv.load_dotenv()

# Environment Variables Configuration
"""
Required Environment Variables:
1. Discord Bot Configuration:
   - DISCORD_BOT_TOKEN: Primary bot token
   - Token: Alternative bot token
   - ID: Alternative bot token
   - URL: URL containing bot token (will extract from query parameter)
   - SERVER_ID: Target server ID for monitoring (optional)
   - MONITOR_GUILD_ID: Guild to monitor for statistics
   - OUTPUT_GUILD_ID: Guild to send output messages
   - OUTPUT_CHANNEL_ID: Channel to send output messages

2. Google Sheets Configuration:
   - GOOGLE_SHEETS_ENABLED: 'true' to enable Google Sheets integration
   - GOOGLE_SHEETS_ID: Main spreadsheet ID
   - VOLUNTARY_LEAVES_SHEET_ID: Spreadsheet ID for voluntary leaves
   - GOOGLE_CREDENTIALS: Base64 encoded service account credentials
   - SHEET_NAME: Sheet name for onboarding tracking (default: 'Sheet1')

3. Schedule Configuration:
   - SCHEDULE_HOUR: Hour for daily stats collection (default: 13)
   - SCHEDULE_MINUTE: Minute for daily stats collection (default: 0)
"""

# Discord Configuration
DISCORD_BOT_TOKEN = os.getenv('DISCORD_BOT_TOKEN')
MONITOR_GUILD_ID = int(os.getenv('MONITOR_GUILD_ID'))
OUTPUT_GUILD_ID = int(os.getenv('OUTPUT_GUILD_ID'))
OUTPUT_CHANNEL_ID = int(os.getenv('OUTPUT_CHANNEL_ID'))
# SERVER_ID = os.getenv('SERVER_ID')

# Google Sheets Configuration
SHEETS_ENABLED = os.getenv('GOOGLE_SHEETS_ENABLED', 'false').lower() == 'true'
# INFO_SPREADSHEET_ID = os.getenv('INFO_GOOGLE_SHEETS_ID')
# JOIN_SPREADSHEET_ID = os.getenv('JOIN_GOOGLE_SHEETS_ID')
# VOLUNTARY_LEAVES_SHEET_ID = os.getenv('VOLUNTARY_LEAVES_SHEET_ID')
# ONBOARDING_SHEET_NAME = os.getenv('SHEET_NAME', 'Sheet1')

MERGED_SHEET_ID = os.getenv('MERGED_SHEET_ID')
SERVER_STATS_SHEET_NAME = os.getenv('SERVER_STATS_SHEET_NAME')
ROLE_STATS_SHEET_NAME = os.getenv('ROLE_STATS_SHEET_NAME')
JOIN_INFO_SHEET_NAME = os.getenv('JOIN_INFO_SHEET_NAME')
VOLUNTARY_LEAVES_SHEET_NAME = os.getenv('VOLUNTARY_LEAVES_SHEET_NAME')

GOOGLE_CREDENTIALS = os.getenv('GOOGLE_CREDENTIALS')

# Schedule Configuration
SCHEDULE_HOUR = int(os.getenv('SCHEDULE_HOUR', '17'))
SCHEDULE_MINUTE = int(os.getenv('SCHEDULE_MINUTE', '0'))

# Constants
JST = ZoneInfo("Asia/Tokyo")
UTC = timezone.utc
VOLUNTARY_LEAVES_FILE = os.path.join(BASE_DIR, VOLUNTARY_LEAVES_SHEET_NAME + '.csv')
ROLE_STATS_FILE = os.path.join(BASE_DIR, ROLE_STATS_SHEET_NAME + '.csv')
SERVER_STATS_FILE = os.path.join(BASE_DIR, SERVER_STATS_SHEET_NAME + '.csv')
LOG_FILE = os.path.join(BASE_DIR, 'discord_bot.log')

# Initialize logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Bot setup
intents = discord.Intents.all()
intents.presences = True
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)
scheduler = AsyncIOScheduler()

# Token retrieval (from bot.py)
def get_token():
    token_vars = ["DISCORD_BOT_TOKEN", "Token", "ID", "URL"]
    for var in token_vars:
        token = os.getenv(var)
        if token and isinstance(token, str):
            if var == "URL" and "token=" in token.lower():
                try:
                    parsed = urlparse(token)
                    query = parse_qs(parsed.query)
                    if "token" in query:
                        token = query["token"][0]
                except Exception:
                    continue
            if not token.startswith(('http://', 'https://')):
                print(f"トークンを環境変数 {var} から読み込みました")
                return token
    return None

TOKEN = get_token()

# Core configuration
# SHEETS_ENABLED = os.getenv('GOOGLE_SHEETS_ENABLED', 'false').lower() == 'true'
# SPREADSHEET_ID = os.getenv('GOOGLE_SHEETS_ID')
# VOLUNTARY_LEAVES_SHEET_ID = os.getenv('VOLUNTARY_LEAVES_SHEET_ID')
# CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS')
# ONBOARDING_SHEET_NAME = os.getenv('SHEET_NAME', 'Sheet1')

# Google Sheets Integration Functions
def get_google_sheets_service():
    """Google Sheets APIサービスを初期化する"""
    try:
        logger.info('Google Sheets APIサービスの初期化を開始...')
        
        if not SHEETS_ENABLED:
            logger.warning('Google Sheets統合が無効に設定されています')
            return None
            
        if not GOOGLE_CREDENTIALS:
            logger.warning('Google Cloud認証情報が設定されていません')
            return None
            
        # if not INFO_SPREADSHEET_ID:
        #     logger.warning('統計情報用のスプレッドシートIDが設定されていません')
        #     return None

        # if not JOIN_SPREADSHEET_ID:
        #     logger.warning('加入情報用のスプレッドシートIDが設定されていません')
        #     return None

        # if not VOLUNTARY_LEAVES_SHEET_ID:
        #     logger.warning('脱退用のスプレッドシートIDが設定されていません')
        #     return None
        
        if not MERGED_SHEET_ID:
            logger.warning('MERGED_SHEET_IDが設定されていません')
            return None

        logger.info('Base64エンコードされた認証情報をデコード中...')
        credentials_json = base64.b64decode(GOOGLE_CREDENTIALS)
        credentials_dict = json.loads(credentials_json)
        
        logger.info('サービスアカウント認証情報を作成中...')
        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        
        logger.info('Google Sheets APIサービスを構築中...')
        service = build('sheets', 'v4', credentials=credentials)
        
        logger.info('Google Sheets APIサービスの初期化が完了しました')
        return service
    except Exception as e:
        logger.error(f'Google Sheetsサービスの初期化に失敗しました: {e}')
        return None

def find_last_row_in_sheet(service, spreadsheet_id: str, sheet_name: str) -> int:
    """指定されたシートの最終行の次の行番号を取得する"""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f'{sheet_name}!A:Z'
        ).execute()
        
        values = result.get('values', [])
        if not values:
            logger.info(f'シート"{sheet_name}"は空です。1行目から書き込みを開始します。')
            return 1
            
        next_row = len(values) + 1
        logger.info(f'シート"{sheet_name}"の最終行: {len(values)}行目')
        logger.info(f'次の書き込み位置: {next_row}行目')
        return next_row
        
    except Exception as e:
        logger.error(f'最終行の取得に失敗しました: {e}')
        logger.warning('安全のため、1行目から書き込みを開始します。')
        return 1

def write_to_sheet_general(target_spreadsheet_id: str, sheet_name: str, values: List[List[Any]], range_name: Optional[str] = None, headers: Optional[List[str]] = None) -> bool:
    """指定されたスプレッドシートの指定シートにデータを書き込む"""
    try:
        logger.info(f'===[START] Writing to Google Sheet "{sheet_name}" (Spreadsheet ID: {target_spreadsheet_id})===')
        
        service = get_google_sheets_service()
        if not service:
            logger.error('Google Sheetsサービスの初期化に失敗したため、データを書き込めません')
            return False

        # スプレッドシートの情報を取得
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=target_spreadsheet_id).execute()
            existing_sheets = [sheet['properties']['title'] for sheet in spreadsheet.get('sheets', [])]
            
            # シートが存在しない場合は作成
            if sheet_name not in existing_sheets:
                logger.info(f'シート"{sheet_name}"が存在しないため、新規作成します')
                request_body = {
                    'requests': [{
                        'addSheet': {
                            'properties': {
                                'title': sheet_name
                            }
                        }
                    }]
                }
                service.spreadsheets().batchUpdate(
                    spreadsheetId=target_spreadsheet_id,
                    body=request_body
                ).execute()
                logger.info(f'シート"{sheet_name}"を作成しました')
        except Exception as e:
            logger.error(f'シートの確認/作成中にエラーが発生しました: {e}')
            return False

        # 書き込み範囲の決定
        if not range_name:
            next_row = find_last_row_in_sheet(service, target_spreadsheet_id, sheet_name)
            
            if not values:
                logger.error('書き込むデータが空です')
                return False
                
            if next_row == 1 and headers:
                values = [headers] + values
                range_name = f'{sheet_name}!A1'
            else:
                range_name = f'{sheet_name}!A{next_row}'

        body = {
            'values': values
        }

        service.spreadsheets().values().append(
            spreadsheetId=target_spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        
        logger.info(f'✅ "{sheet_name}"シートへのデータ書き込みが完了しました')
        logger.info(f'===[COMPLETE] Successfully wrote {len(values)} rows to {sheet_name}===')
        return True
    except HttpError as e:
        logger.error(f'✖ Google Sheetsへの書き込み中にエラーが発生しました: {e}')
        logger.error(f'===[FAILED] Failed to write to {sheet_name}===')
        return False

def write_to_sheet(sheet_name: str, values: List[List[Any]], range_name: Optional[str] = None, headers: Optional[List[str]] = None) -> bool:
    """指定されたシートにデータを書き込む（デフォルトのスプレッドシートを使用）"""
    if not MERGED_SHEET_ID:
        logger.error('スプレッドシートIDが設定されていません')
        return False
    return write_to_sheet_general(MERGED_SHEET_ID, sheet_name, values, range_name, headers=headers)

def write_to_spreadsheet(username: str, status: str, error_message: str = "", current_roles: str = "") -> bool:
    """ユーザーのロール情報をスプレッドシートに記録する（オンボーディング用）"""
    print("\n=== スプレッドシート書き込み開始 ===")
    print(f"- ユーザー名: {username}")
    print(f"- ステータス: {status}")
    print(f"- エラー: {error_message if error_message else 'なし'}")
    print(f"- 現在のロール: {current_roles if current_roles else 'なし'}")
    
    try:
        service = get_google_sheets_service()
        if not service:
            return False

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"- タイムスタンプ: {timestamp}")
        
        row = [timestamp, username, status, error_message, current_roles]
        range_name = f'{JOIN_INFO_SHEET_NAME}!A:A'
        body = {
            'values': [row]
        }
        
        result = service.spreadsheets().values().append(
            spreadsheetId=MERGED_SHEET_ID,
            range=range_name,
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        
        print(f"✅ スプレッドシート書き込み成功: {row}")
        return True
    except Exception as e:
        print(f"❌ スプレッドシートの書き込み中にエラーが発生しました: {type(e).__name__} - {str(e)}")
        return False

# Statistics Collection Functions
async def get_guild_stats():
    """サーバーの統計情報を収集する"""
    await bot.wait_until_ready()
    
    try:
        # monitor_guild_id_str = os.getenv('MONITOR_GUILD_ID')
        monitor_guild_id_str = MONITOR_GUILD_ID
        if not monitor_guild_id_str:
            print('エラー: MONITOR_GUILD_IDが設定されていません')
            return None
        monitor_guild_id = int(monitor_guild_id_str)
        guild = bot.get_guild(monitor_guild_id)
        
        if not guild:
            print('エラー: ギルドが見つかりませんでした。')
            return None
        
        now = datetime.now(JST)
        yesterday_start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=JST)
        yesterday_end = (now - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=JST)
        
        current_members = guild.member_count
        
        join_count = 0
        for member in guild.members:
            if member.joined_at:
                joined_at = member.joined_at.replace(tzinfo=UTC) if member.joined_at.tzinfo is None else member.joined_at
                if yesterday_start <= joined_at <= yesterday_end:
                    join_count += 1
        
        leave_count = 0
        voluntary_leave_count = 0
        forced_leave_count = 0
        forced_leave_ids = set()
        
        # 強制退会の確認（監査ログから）
        try:
            actions = [
                discord.AuditLogAction.kick,
                discord.AuditLogAction.ban,
                discord.AuditLogAction.member_prune
            ]
            
            for action in actions:
                after = yesterday_start
                while True:
                    entries = []
                    try:
                        async for entry in guild.audit_logs(action=action, limit=100, after=after):
                            entries.append(entry)
                    except discord.Forbidden:
                        logger.warning(f'監査ログ（{action.name}）へのアクセス権限がありません')
                        continue
                    
                    if not entries:
                        break
                        
                    for entry in entries:
                        created_at = entry.created_at.replace(tzinfo=UTC) if entry.created_at and entry.created_at.tzinfo is None else entry.created_at
                        
                        if not created_at or created_at > yesterday_end:
                            break
                            
                        if yesterday_start <= created_at <= yesterday_end:
                            if action == discord.AuditLogAction.member_prune:
                                forced_leave_count += entry.extra.members
                            else:
                                if hasattr(entry.target, 'id'):
                                    forced_leave_ids.add(str(entry.target.id))
                                forced_leave_count += 1
                    
                    if entries:
                        after = entries[-1].created_at
                    else:
                        break
                        
        except discord.Forbidden:
            logger.warning('監査ログへのアクセス権限がありません')
            forced_leave_ids.clear()

        # 自主退会の確認（CSVファイルから）
        if os.path.exists(VOLUNTARY_LEAVES_FILE):
            try:
                with open(VOLUNTARY_LEAVES_FILE, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            leave_time = datetime.fromisoformat(row['timestamp'])
                            if leave_time.tzinfo is None:
                                leave_time = leave_time.replace(tzinfo=UTC)
                            
                            user_id = row['user_id']
                            
                            if yesterday_start <= leave_time <= yesterday_end:
                                if user_id not in forced_leave_ids:
                                    voluntary_leave_count += 1
                        except (ValueError, KeyError) as e:
                            logger.error(f"退会データの解析中にエラーが発生しました: {e}")
                            continue
            except Exception as e:
                logger.error(f"{VOLUNTARY_LEAVES_FILE} の読み込み中にエラーが発生しました: {e}")
        
        leave_count = voluntary_leave_count + forced_leave_count
        
        # アクティブメンバー数の取得
        active_members = set()
        for channel in guild.text_channels:
            try:
                async for message in channel.history(limit=100, after=yesterday_start, before=yesterday_end):
                    if not message.author.bot:
                        active_members.add(message.author.id)
            except discord.Forbidden:
                continue
        
        active_count = len(active_members)
        
        return {
            'date': yesterday_start.astimezone(JST).strftime('%Y-%m-%d'),
            'current_members': current_members,
            'new_members': join_count,
            'left_members': leave_count,
            'voluntary_leaves': voluntary_leave_count,
            'forced_leaves': forced_leave_count,
            'active_members': active_count
        }
        
    except Exception as e:
        logger.error(f'エラー: 予期せぬエラーが発生しました: {e}')
        return None

async def get_role_stats(guild: discord.Guild):
    """サーバー内のロールごとのメンバー数をCSVに出力する"""
    try:
        current_date = (datetime.now(JST) - timedelta(days=1)).strftime('%Y-%m-%d')
        csv_filename = ROLE_STATS_FILE
        
        role_data = {}
        for role in guild.roles:
            if not role.is_default():
                role_data[role.name] = len(role.members)
        
        fieldnames = ['Date'] + sorted(role_data.keys())
        new_file = not os.path.exists(csv_filename)
        
        current_data = {'Date': current_date}
        current_data.update(role_data)

        if SHEETS_ENABLED:
            sheet_data = [
                [current_date] + [role_data[role] for role in sorted(role_data.keys())]
            ]
            
            sheet_name = ROLE_STATS_SHEET_NAME
            if write_to_sheet(sheet_name, sheet_data, headers=fieldnames):
                logger.info(f'ロール統計情報をGoogle Sheetsに出力しました (シート名: {sheet_name})')
            else:
                logger.warning('Google Sheetsへの書き込みに失敗しました')

        with open(csv_filename, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if new_file:
                writer.writeheader()
            writer.writerow(current_data)


        return True

    except Exception as e:
        logger.error(f'エラー: ロール統計情報の処理中にエラーが発生しました: {e}')
        return False

async def process_stats(ctx=None):
    """統計情報の収集、CSV出力、送信を行う"""
    try:
        if ctx:
            await ctx.send('Discord APIに接続しています...')
        stats = await get_guild_stats()
        
        if stats is None:
            error_msg = ('エラー: Discord APIからの統計情報の取得に失敗しました。')
            if ctx:
                await ctx.send(error_msg)
            return
            
        fieldnames = ['Date', 'Total Members', 'New Members', 'Total Leaves', 'Voluntary Leaves', 'Forced Leaves', 'Active Members']
        row_data = {
            'Date': stats['date'],
            'Total Members': stats['current_members'],
            'New Members': stats['new_members'],
            'Total Leaves': stats['left_members'],
            'Voluntary Leaves': stats['voluntary_leaves'],
            'Forced Leaves': stats['forced_leaves'],
            'Active Members': stats['active_members']
        }

        csv_filename = SERVER_STATS_FILE
        new_file = not os.path.exists(csv_filename)
        with open(csv_filename, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if new_file:
                writer.writeheader()
            writer.writerow(row_data)

        if SHEETS_ENABLED:
            sheet_data = [list(row_data.values())]
            sheet_name = SERVER_STATS_SHEET_NAME
            if write_to_sheet(sheet_name, sheet_data, headers=fieldnames):
                logger.info(f'サーバー統計情報をGoogle Sheetsに出力しました (シート名: {sheet_name})')

        monitor_guild_id = MONITOR_GUILD_ID
        guild = bot.get_guild(monitor_guild_id)
        if guild:
            await get_role_stats(guild)

        message = (
            f"【Discordサーバー統計情報】\n"
            f"日付: {stats['date']}\n\n"
            f"1. サーバー状況\n"
            f"   - 現在のメンバー数: {stats['current_members']}人\n"
            f"   - 新規参加者数: {stats['new_members']}人\n"
            f"   - 退会者数: {stats['left_members']}人\n"
            f"     ├ 自主退会: {stats['voluntary_leaves']}人\n"
            f"     └ 強制退会: {stats['forced_leaves']}人\n"
            f"   - アクティブメンバー数: {stats['active_members']}人\n\n"
            f"※このメッセージは自動生成されています。"
        )
        
        if ctx:
            await ctx.send(message)
        else:
            output_guild_id = OUTPUT_GUILD_ID
            output_channel_id = OUTPUT_CHANNEL_ID
            output_guild = bot.get_guild(output_guild_id)
            
            if output_guild:
                channel = output_guild.get_channel(output_channel_id)
                if channel:
                    await channel.send(message)
    
    except Exception as e:
        logger.error(f'エラー: 予期せぬエラーが発生しました: {e}')

# Bot Event Handlers
@bot.command(name='stats')
async def stats_command(ctx, *, arg: Optional[str] = None):
    """統計情報を手動で収集するコマンド"""
    if arg and "--time" in arg:
        time_str = arg.replace("--time", "").strip()
        try:
            hour, minute = map(int, time_str.split(":"))
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError("Invalid time range")
            
            start_date = datetime.now(UTC)
            target_time = start_date.replace(hour=hour, minute=minute, tzinfo=UTC)
            if target_time < start_date:
                start_date = start_date + timedelta(days=1)
            
            trigger = CronTrigger(hour=hour, minute=minute, start_date=start_date)
            scheduler.add_job(
                process_stats,
                trigger,
                id='manual_stats_job',
                replace_existing=True
            )
            next_run = start_date.replace(hour=hour, minute=minute)
            await ctx.send(f"{next_run.strftime('%Y-%m-%d %H:%M')}に統計情報を出力するようスケジュールを設定しました。")
        except ValueError:
            await ctx.send("時刻の指定に失敗しました。正しい形式で指定してください。例: !stats --time 15:00")
    else:
        await ctx.send("統計情報をただちに収集します...")
        await process_stats(ctx)

@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    """メンバーの更新イベントを処理し、オンボーディング完了時にロール情報をスプレッドシートに記録する"""
    try:
        # サーバーID確認
        server_id = MONITOR_GUILD_ID
        if server_id and (not after.guild or str(after.guild.id) != str(MONITOR_GUILD_ID)):
            return

        # オンボーディング完了フラグの確認
        if not hasattr(before.flags, 'completed_onboarding') or not hasattr(after.flags, 'completed_onboarding'):
            return
            
        if not before.flags.completed_onboarding and after.flags.completed_onboarding:
            try:
                display_name = after.display_name
                roles_list = [role.name for role in after.roles if not role.is_default()]
                roles_str = ",".join(roles_list)
                
                write_to_spreadsheet(
                    username=display_name,
                    status="SUCCESS",
                    current_roles=roles_str
                )
            except Exception as e:
                error_message = f"オンボーディング処理中にエラーが発生しました: {type(e).__name__} - {str(e)}"
                try:
                    display_name = after.display_name
                except:
                    display_name = "不明なユーザー"
                
                write_to_spreadsheet(
                    username=display_name,
                    status="ERROR",
                    error_message=error_message
                )
    except Exception as e:
        error_message = f"オンボーディング処理中にエラーが発生しました: {type(e).__name__} - {str(e)}"
        write_to_spreadsheet(
            username="不明なユーザー",
            status="ERROR",
            error_message=error_message
        )

@bot.event
async def on_member_remove(member):
    """メンバーが退会した際のイベントハンドラー"""
    try:
        logger.info("=== on_member_remove イベント開始 ===")
        # departed_at = datetime.now(UTC)
        departed_at = datetime.now(JST)
        departed_at_str = departed_at.strftime('%Y-%m-%d %H:%M:%S %Z')
        roles = [role.name for role in member.roles if role.name != '@everyone']
        roles_str = '、'.join(roles) if roles else 'なし'

        file_exists = os.path.exists(VOLUNTARY_LEAVES_FILE)
        try:
            with open(VOLUNTARY_LEAVES_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(['timestamp', 'user_id', 'user_name', 'roles'])
                writer.writerow([
                    departed_at.isoformat(),
                    str(member.id),
                    member.name,
                    roles_str
                ])
            
            if SHEETS_ENABLED:
                await write_voluntary_leaves_to_sheet()
        except Exception as e:
            logger.error(f"退会情報のCSV記録中にエラーが発生しました: {e}")

        log_message = [
            '【メンバー退会情報】',
            f'ユーザー名: {member.name}',
            f'ユーザーID: {member.id}',
            f'退会日時: {departed_at_str}',
            f'保持していたロール: {roles_str}'
        ]

        try:
            async for entry in member.guild.audit_logs(action=discord.AuditLogAction.kick, limit=1):
                if entry.target.id == member.id:
                    log_message.extend([
                        '退会種別: キック',
                        f'実行者: {entry.user.name}'
                    ])
                    if entry.reason:
                        log_message.append(f'理由: {entry.reason}')
                    break
            else:
                log_message.append('退会種別: 自主退会')
        except discord.Forbidden:
            log_message.append('警告: 監査ログへのアクセス権限がありません')

        try:
            output_guild_id = OUTPUT_GUILD_ID
            output_channel_id = OUTPUT_CHANNEL_ID
            output_guild = bot.get_guild(output_guild_id)
            
            if output_guild:
                channel = output_guild.get_channel(output_channel_id)
                if channel:
                    await channel.send('\n'.join(log_message))
        except Exception as e:
            logger.error(f'エラー: メッセージの送信中にエラーが発生しました: {e}')

    except Exception as e:
        logger.error(f'エラー: 退会情報の処理中にエラーが発生しました: {e}')

async def write_voluntary_leaves_to_sheet() -> bool:
    """自主退会者情報を専用のスプレッドシートに出力する"""
    logger.info("=== 自主退会者スプレッドシートへの出力開始 ===")
    
    if not SHEETS_ENABLED or not MERGED_SHEET_ID or not os.path.exists(VOLUNTARY_LEAVES_FILE):
        return False
    
    # data_rows = [] # removed 02/19
    # headers = ['タイムスタンプ', 'ユーザーID', 'ユーザー名', '退会前のロール']
    headers = ['timestamp', 'user_id', 'user_name', 'roles']
    
    try:
        with open(VOLUNTARY_LEAVES_FILE, 'r', encoding='utf-8') as f:
            csv_reader = csv.DictReader(f)
            last_row = None # added 02/19

            for row in csv_reader:
                last_row = row # added 02/19

            timestamp = datetime.fromisoformat(row['timestamp'])
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)
            jst_timestamp = timestamp.astimezone(JST)
            
            # removed 02/19
            # data_rows.append([
            #     jst_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            #     row['user_id'],
            #     row['user_name'],
            #     row['roles']
            # ])

            # added 02/19
            # Format the last row data
            data_row = [
                jst_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                last_row['user_id'],
                last_row['user_name'],
                last_row['roles']
            ]
    
    except Exception as e:
        logger.error(f"自主退会者CSVの読み込み中にエラーが発生しました: {e}")
        return False
    
    sheet_name = "退会者統計"
    # return write_to_sheet_general(MERGED_SHEET_ID, VOLUNTARY_LEAVES_SHEET_NAME, data_rows, headers=headers) # removed 02/19
    return write_to_sheet_general(MERGED_SHEET_ID, VOLUNTARY_LEAVES_SHEET_NAME, [data_row], headers=headers) # added 02/19

@bot.event
async def on_ready():
    """Bot起動時の初期化処理"""
    logger.info("=== BOTの起動とログイン ===")
    logger.info(f"Bot名: {bot.user}")
    logger.info(f"Bot ID: {bot.user.id}")
    logger.info(f"Discord.py バージョン: {discord.__version__}")
    
    print('\n=== 接続情報 ===')
    monitor_guild = bot.get_guild(int(MONITOR_GUILD_ID))
    if monitor_guild:
        print(f'  - 名前: {monitor_guild.name}')
        print(f'  - メンバー数: {monitor_guild.member_count}')
    
    output_guild = bot.get_guild(int(OUTPUT_GUILD_ID))
    if output_guild:
        channel = output_guild.get_channel(int(OUTPUT_CHANNEL_ID))
        print(f'  - 名前: {output_guild.name}')
        print(f'  - 出力チャンネル: {channel.name if channel else "見つかりません"}')
    
    schedule_hour = int(SCHEDULE_HOUR)
    schedule_minute = int(SCHEDULE_MINUTE)

    scheduler.add_job(
        process_stats,
        CronTrigger(hour=schedule_hour, minute=schedule_minute),
        id='discord_stats_job',
        name='Discord統計情報収集',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info(f'定期実行の設定が完了しました（毎日{schedule_hour}時{schedule_minute}分に実行）')
    
    await process_stats()



# Main execution


# if __name__ == "__main__":
#     if not TOKEN:
#         print("エラー: 有効なトークンが見つかりませんでした。")
#         print("\n環境変数の状態:")
#         for var in ["DISCORD_BOT_TOKEN", "Token", "ID", "URL"]:
#             value = os.getenv(var)
#             if value:
#                 if var == "URL":
#                     print(f"- {var}: {value}")
#                 else:
#                     masked_value = value[:6] + "*" * (len(value) - 6) if len(value) > 6 else "***"
#                     print(f"- {var}: {masked_value}")
#             else:
#                 print(f"- {var}: 未設定")
#         sys.exit(1)
        
# server_id = os.getenv("MONITOR_GUILD_ID")
server_thread()
server_id = MONITOR_GUILD_ID
TOKEN = DISCORD_BOT_TOKEN
if not server_id:
    print("警告: MONITOR_GUILD_IDが設定されていません。すべてのサーバーのメッセージを処理します。")

try:
    print(f"Botを起動しています...")
    bot.run(TOKEN)
except discord.LoginFailure as e:
    print("\n=== ログインエラー ===")
    print("エラー: Discordへのログインに失敗しました")
    print(f"\nエラーの詳細: {str(e)}")
except discord.HTTPException as e:
    print("\n=== 接続エラー ===")
    print("エラー: Discord APIとの通信に失敗しました")
    print(f"\nエラーの詳細: {str(e)}")
except Exception as e:
    print("\n=== システムエラー ===")
    print("エラー: 予期せぬ問題が発生しました")
    print(f"エラーの種類: {type(e).__name__}")
    print(f"エラーの詳細: {str(e)}")

