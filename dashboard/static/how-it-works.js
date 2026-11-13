const flowSteps = [
  "The producer publishes an event to a named topic.",
  "The broker appends the event to a partition and assigns it the next offset.",
  "A consumer group reads its assigned partition and commits the offset after processing.",
];

const nodes = [...document.querySelectorAll(".flow-node")];
const arrows = [...document.querySelectorAll(".flow-arrow")];
const stepLabel = document.getElementById("flowStep");
const description = document.getElementById("flowDescription");
const restartButton = document.getElementById("restartFlow");
let currentStep = 0;
let timer;

function showStep(step) {
  currentStep = step;
  nodes.forEach((node, index) => node.classList.toggle("active", index === step));
  arrows.forEach((arrow, index) => arrow.classList.toggle("active", index < step));
  stepLabel.textContent = `Step ${step + 1} of ${flowSteps.length}`;
  description.textContent = flowSteps[step];
}

function play() {
  clearInterval(timer);
  showStep(0);
  timer = setInterval(() => {
    showStep((currentStep + 1) % flowSteps.length);
  }, 3000);
}

restartButton.addEventListener("click", play);
play();
