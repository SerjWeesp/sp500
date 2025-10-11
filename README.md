import pandas as pd
from collections import defaultdict

# --- helpers from your plotting module (kept here for completeness) ---
def iter_action_successors(action_obj, pre_state=None, prob_threshold=0.0):
    """
    Yields (post_state, p) for a given action.
    Supports two shapes seen in screenshots:
      A) probabilities[pre_state][post_state] = p
      B) probabilities[post_state] = p  (independent of pre_state)
    """
    probs = getattr(action_obj, "probabilities", {}) or {}
    # Shape A?
    if any(isinstance(v, dict) for v in probs.values()):
        row = probs.get(pre_state, {}) if pre_state is not None else {}
        for post_s, p in row.items():
            if p and float(p) >= prob_threshold:
                yield post_s, float(p)
    else:
        # Shape B
        for post_s, p in probs.items():
            if p and float(p) >= prob_threshold:
                yield post_s, float(p)

def all_from_states(asc):
    """Collect every state that appears in actions' from_states (best-effort)."""
    res = set()
    for _, a_obj in getattr(asc, "actions", {}).items():
        fs = getattr(a_obj, "from_states", None)
        if fs:
            res.update(fs)
    return res

def is_terminal_state(asc, s, from_states_cache=None):
    """Conservative terminal check (matches your screenshot logic)."""
    st = asc.states.get(s)
    if st is None:
        return True
    if getattr(st, "is_terminal", False):
        return True
    if not getattr(st, "actions", []):
        return True
    if from_states_cache is not None and s not in from_states_cache:
        # never appears in any "from_states"
        return True
    return False
# ----------------------------------------------------------------------

def _from_states_for_action(asc, a_name, a_obj):
    """
    Resolve which pre-states feed this action.
    1) Prefer explicit state->actions wiring (asc.states[s].actions)
    2) Fallback: keys of probabilities when in shape-A
    3) Otherwise: [None] meaning 'independent of state'
    """
    fs = sorted([s for s, sobj in asc.states.items()
                 if getattr(sobj, "actions", None) and a_name in sobj.actions])
    if fs:
        return fs

    probs = getattr(a_obj, "probabilities", {}) or {}
    if probs and any(isinstance(v, dict) for v in probs.values()):
        return list(probs.keys())

    return [None]

def asc_to_transition_table(asc, *, ascmdp=None, gamma=None, prob_threshold=0.0):
    """
    Build a flat table of transitions for a single (scenario, actor) ASC.
    If `ascmdp` is provided, rewards are computed via ascmdp.R(s,a,s').
    Otherwise we try per-action rewards / rewards_dev mappings when present.
    """
    rows = []
    from_states_cache = all_from_states(asc)

    for a_name, a_obj in asc.actions.items():
        for s in _from_states_for_action(asc, a_name, a_obj):
            for s_prime, p in iter_action_successors(a_obj, pre_state=s, prob_threshold=prob_threshold):
                # --- probability ---
                prob = float(p)

                # --- reward (actor utility) ---
                reward = None
                reward_dev = None
                if ascmdp is not None and hasattr(ascmdp, "R"):
                    # actor-utility reward from your MDP wrapper
                    try:
                        reward = float(ascmdp.R(s, a_name, s_prime))
                    except Exception:
                        reward = None
                else:
                    # fallback to action-local reward mappings, if any
                    rw = getattr(a_obj, "rewards", {})
                    if isinstance(rw, dict):
                        if s is not None and isinstance(rw.get(s), dict):
                            reward = float(rw.get(s, {}).get(s_prime, 0.0))
                        else:
                            reward = float(rw.get(s_prime, 0.0))
                    rw_dev = getattr(a_obj, "rewards_dev", {})
                    if isinstance(rw_dev, dict):
                        if s is not None and isinstance(rw_dev.get(s), dict):
                            reward_dev = float(rw_dev.get(s, {}).get(s_prime, 0.0))
                        else:
                            reward_dev = float(rw_dev.get(s_prime, 0.0))

                # --- misc flags / masks if present on the action ---
                noisy_mask = getattr(a_obj, "noisy_prob_mask", None)

                # --- terminal flag (for convenience) ---
                terminal = is_terminal_state(asc, s_prime, from_states_cache)

                rows.append({
                    "action_name": a_name,
                    "action_from_state": s,
                    "to_state": s_prime,
                    "prob": prob,
                    "reward": reward,
                    "reward_dev": reward_dev,
                    "discount": gamma,
                    "noisy_prob_mask": noisy_mask,
                    "is_terminal": terminal,
                })

    df = pd.DataFrame(rows).sort_values(
        ["action_name", "action_from_state", "to_state"], na_position="first"
    ).reset_index(drop=True)
    return df
