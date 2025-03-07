class HelloWorld {
    public static void main(String[] args) {
        System.out.println("Hello, World!");

        // Keep the program running indefinitely
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            System.err.println("Application interrupted");
        }
    }
}
