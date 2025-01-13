from fastapi import FastAPI,Depends,status,HTTPException,Request, Form
from typing import Annotated,Optional
from sqlalchemy.orm import Session,Query
from database import get_db
from models import *
from datetime import date
from routers import players
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy import or_,cast

app= FastAPI(title="Tennis stats")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

db_dependency=Annotated[Session,Depends(get_db)]

app.include_router(players.router,prefix="/players",tags=["players"])

@app.get("/",status_code=status.HTTP_200_OK)
async def get_first_10_singles_matches(db:db_dependency):
    return db.query(AtpSingles).limit(10).all()

# main.py
from fastapi import FastAPI, Depends, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy import or_
from typing import Optional

app = FastAPI(title="Tennis stats")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/head-to-head")
async def head_to_head_page(request: Request):
    """Render the head to head search page"""
    return templates.TemplateResponse(
        "head_to_head.html",
        {"request": request}
    )

@app.get("/search-players")
async def search_players(
    request: Request,
    query: str,
    db: db_dependency
):
    """AJAX endpoint for player search"""
    if len(query) < 2:
        return []
        
    search_query = f"%{query}%"
    players = db.query(AtpPlayers).filter(
        or_(
            AtpPlayers.name_full.ilike(search_query),
            AtpPlayers.name_first.ilike(search_query),
            AtpPlayers.name_last.ilike(search_query)
        )
    ).limit(10).all()
    
    return [
        {
            "id": p.player_id,
            "name": p.name_full,
            "country": p.ioc
        } for p in players
    ]

@app.post("/head-to-head-results")
async def get_head_to_head_results(
    db:db_dependency,
    request: Request,
    player1_id: int = Form(...),
    player2_id: int = Form(...),
):
    player1 = db.query(AtpPlayers).filter(cast(AtpPlayers.player_id,BigInteger) == player1_id).first()
    player2 = db.query(AtpPlayers).filter(cast(AtpPlayers.player_id,BigInteger) == player2_id).first()
    
    if not player1 or not player2:
        return templates.TemplateResponse(
            "head_to_head.html",
            {
                "request": request,
                "error": "One or both players not found"
            }
        )
    
    # Get head to head stats
    p1_wins = db.query(AtpSingles).filter(
        cast(AtpSingles.winner_id, BigInteger) == player1_id,
        cast(AtpSingles.loser_id, BigInteger) == player2_id
    ).count()
    
    p2_wins = db.query(AtpSingles).filter(
        cast(AtpSingles.winner_id,BigInteger) == player2_id,
        cast(AtpSingles.loser_id, BigInteger) == player1_id
    ).count()
    
    # Get recent matches
    recent_matches = db.query(AtpSingles).filter(
        or_(
            (cast(AtpSingles.winner_id,BigInteger) == player1_id) & (cast(AtpSingles.loser_id,BigInteger) == player2_id),
            (cast(AtpSingles.winner_id,BigInteger) == player2_id) & (cast(AtpSingles.loser_id,BigInteger) == player1_id)
        )
    ).order_by(AtpSingles.match_num.desc()).limit(5).all()
    
    return templates.TemplateResponse(
        "head_to_head.html",
        {
            "request": request,
            "player1": player1,
            "player2": player2,
            "p1_wins": p1_wins,
            "p2_wins": p2_wins,
            "total_matches": p1_wins + p2_wins,
            "recent_matches": recent_matches
        }
    )






