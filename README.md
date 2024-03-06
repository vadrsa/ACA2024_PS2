# Testing the solution
The solution comes with a set of unit tests, that you should try to take advantage of. Here are some of the advantages of using test driven development(TDD):

- **Error Detection:** Tests help identify errors and issues in your code early in the development cycle, saving time and effort in the long run.
- **Understanding Requirements:** They ensure your implementation meets the specified requirements, acting as a guideline for the functionality your code must achieve.
- **Learning and Improvement:** Engaging with tests enhances your problem-solving and coding skills, teaching you to write more robust and maintainable code.
- **Preparation for Real-World Development:** The practice mirrors real-world software development environments where reliability and quality are paramount.

## How to Run the Unit Tests

### Prerequisites
Before you can effectively run the unit tests and work on the "GalacticArchive.IndexingEngine" project, there are a few prerequisites you'll need to meet. One of the key requirements is having Docker installed on your machine.

### Installing Docker

1. **Download Docker:**
   - Visit the [Docker website](https://www.docker.com/get-started) and download the Docker Desktop application for your operating system.

2. **Install Docker:**
   - After downloading, run the installer and follow the on-screen instructions to complete the installation process. 
   - For Windows users, Docker Desktop requires the WSL 2 feature and might prompt you to enable it during installation. Follow the provided instructions to set up WSL 2 if necessary.

3. **Verify Installation:**
   - To ensure Docker has been installed correctly, open a terminal or command prompt and run the following command:
     ```
     docker --version
     ```
   - This command should return the installed version of Docker, indicating that it is correctly installed and ready to use.


### Using an IDE (e.g., Visual Studio)

- **Test Explorer:**
  - Go to `Test` > `Windows` > `Test Explorer` to open the Test Explorer pane.
- **Running Tests:**
  - In the Test Explorer, you can run all tests by clicking `Run All` or select specific tests and run them individually.
- **Viewing Results:**
  - The Test Explorer window displays the outcome of each test run, marking them as passed (green tick) or failed (red cross), along with detailed error messages for failures.

### Using the Command Line

- **Navigate to Test Project Directory:**
  - Open a command prompt or terminal window and navigate to the directory containing the "GalacticArchive.IndexingEngine.Tests" project file.
    ```
    cd path\to\your\project\GalacticArchive.IndexingEngine.Tests
    ```
- **Run the Tests:**
  - Execute the following command:
    ```
    dotnet test
    ```
  - This command compiles the tests and the project, runs the tests, and displays the results directly in the terminal.


# Working on the Problem Set via GitHub

This guide outlines the steps to efficiently work on Problem Sets using GitHub. By following these steps, you will fork the assignment repository, clone it to your local machine, work on the problems, push your changes, and finally, submit a pull request to the original repository.

## Step 1: Fork the Repository

1. Navigate to the GitHub page of the Problem Set repository.
2. In the top-right corner of the page, click the **Fork** button.
3. Choose your GitHub account as the location to fork the repository to.

By forking the repository, you create a personal copy of the project on your GitHub account, allowing you to make changes without affecting the original repository.

## Step 2: Clone the Forked Repository

1. On your GitHub page for the forked repository, click the **Code** button and copy the URL provided.
2. Open a terminal or command prompt on your local machine.
3. Change the current working directory to the location where you want the cloned directory.
4. Type `git clone`, and then paste the URL you copied earlier. It should look something like this:

    ```
    git clone https://github.com/your_username/ACA2024_PS1.git
    ```

5. Press **Enter** to create your local clone.

This step creates a local copy of your forked repository, making it easier to work on the Problem Set.

## Step 3: Work on the Problems

1. Navigate to the cloned repository directory on your local machine.
2. Open the Problem Set files using your preferred text editor or IDE.
3. Start working on the Problem Set.
4. Once you've made changes, save your files.

## Step 4: Push the Changes

After you have completed the Problem Set and are satisfied with your solutions, it's time to push your changes back to your forked repository on GitHub.

1. Open a terminal or command prompt.
2. Navigate to your local project directory.
3. Use the `git add` command to stage your changes for commit:

    ```
    git add .
    ```

4. Commit your changes with a message using the `git commit` command:

    ```
    git commit -m "Complete Problem Set"
    ```

5. Push your changes to GitHub:

    ```
    git push origin main
    ```

## Step 5: Create a Pull Request (PR)

Finally, you'll want to submit your Problem Set for review by creating a pull request to the original repository.

1. Navigate to your forked repository on GitHub.
2. Click the **Pull Request** button.
3. Click the **New Pull Request** button.
4. Ensure the base repository is the original repository you forked from and the base branch is `main`.
5. Confirm the head repository is your fork and the compare branch contains your changes.
6. Click **Create Pull Request**.
7. Give your pull request a title and description, outlining the changes you've made or any specific details the reviewer should know.
8. Click **Create Pull Request** again to submit.

Congratulations! You've successfully submitted your Problem Set via GitHub.
