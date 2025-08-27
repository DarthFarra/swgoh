from typing import Any, Dict, List, Tuple, Optional
print(f"[WARN] /player {pid or ally}: {e}")


pname = (m.get("playerName") or m.get("name") or p.get("name") or "").strip() or str(ally or pid)
role_val = int(m.get("memberLevel") or 2)
role = "Leader" if role_val==4 else ("Officer" if role_val==3 else "Member")
ally = str(ally) if ally not in (None, "") else ""


gac = _gac_league_str(p)
by_unit, by_skill = _build_roster_indices(p)


# GP
gp = m.get("galacticPower") or m.get("gp") or p.get("galacticPower") or p.get("statistics", {}).get("galacticPower") or 0
try: gp = int(gp)
except: gp = 0


# leyendas (usando Characters/Ships no aplica; si quieres, añade set GL)
legends = 0 # placeholder si quieres reactivar GL detection


# omicrones (conteo simple por presencia de skill con omicron definido)
# si quieres precisión tier>=omicron_tier, hay que cruzar con by_skill
omi_buckets = {k:0 for k in OMICRON_BUCKETS}
# omitimos parseo completo por brevedad; puedes pegar tu cálculo si lo necesitas aquí


guild_tw_omicrons += omi_buckets.get("TW", 0)
last_raid_score_player = str(raid_score_by_player.get(pid, "")) if pid in raid_score_by_player else ""


players_rows.append([gname, pname, pid, ally, str(gp), role, gac, str(legends), last_raid_score_player,
str(omi_buckets['TW']), str(omi_buckets['TB']), str(omi_buckets['GAC']),
str(omi_buckets['RAID']), str(omi_buckets['CONQ']), str(omi_buckets['CHAL'])])


# units matrix
row_u = [gname, pname]
for b in unit_ids:
ru = by_unit.get(b)
if not ru: row_u.append("")
else: row_u.append("Nave" if b in ship_ids else _format_unit_cell(ru))
units_rows.append(row_u)


# skills matrix
row_s = [gname, pname]
for sid in skill_ids:
tier = by_skill.get(sid)
row_s.append(str(tier) if isinstance(tier, int) else "")
skills_rows.append(row_s)


# fallback guild raid points (suma jugadores) si no vino desde /guild
raid_sum = sum(raid_score_by_player.get(x,0) for x in guild_player_ids)
guilds_rows.append([gid, gname, str(mcount), str(ggp), last_raid_id, (last_raid_points or (str(raid_sum) if raid_sum else "")), str(guild_tw_omicrons)])


# escribir hojas
write_sheet(ws_guilds, ["Guild Id","Guild Name","Number of members","Guild GP","Last Raid Id","Last Raid Score","TW omicrons"], guilds_rows)
write_sheet(ws_players, ["Guild Name","Player Name","Player Id","Ally code","GP","Role","GAC League","Numero de leyendas","Ultima puntuación en raid","Omicrones de GT","Omicrones de BT","Omicrones de GAC","Omicrones de Raid","Omicrones de Conquista","Omicrones de Desafios"], players_rows)
write_sheet(ws_units, ["Player Guild","Player Name", *headers_units], units_rows)
write_sheet(ws_skills, ["Player Guild","Player Name", *skill_ids], skills_rows)


return {"guilds": len(guilds_rows), "players": len(players_rows), "units_rows": len(units_rows), "skills_rows": len(skills_rows)}


if __name__ == "__main__":
print(run())
