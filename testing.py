dialogue = []
MAX_HISTORY = 6
def manage_context(role, msg):
    global dialogue
    dialogue.append({"role": role, "content": msg})
    if len(dialogue) > MAX_HISTORY:
        dialogue = dialogue[-MAX_HISTORY:]
    
    return dialogue