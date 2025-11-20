from requests import RequestException

from api_client import get_todos, get_todo, create_todo

def my_print(todo: dict):
    status = "Good" if todo.get("completed") else " "
    print(f"[{status}] ({todo['id']}) {todo['title']} (user={todo['userId']})")

def main():
    try:
        print(f"First five todos..")
        todos = get_todos(user_id=1)
        for todo in todos[:4]:
            my_print(todo)

        print(f"\nFirst todo..")
        todo = get_todo(user_id=1)
        my_print(todo)

        print(f"\nCreating my todo..")
        new_todo = create_todo(user_id=1, title="Python requests", completed=False)
        print(new_todo)
    except RequestException as e:
        print(e)

if __name__ == "__main__":
    main()