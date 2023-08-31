::sbp_7b909cd6b8218c1ce7e750f419d3be9e557d3833

:: surround with double-quotes, escape internal double-quotes

curl -i --location --request POST "http://localhost:54321/functions/v1/hello" --header "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0" --header "Content-Type: application/json" --data "{\"name\":\"Functions\"}"