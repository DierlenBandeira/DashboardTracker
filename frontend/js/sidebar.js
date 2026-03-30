async function loadSidebar() {
  const root = document.getElementById('sidebar-root');
  if (!root) return;

  try {
    const response = await fetch('/components/sidebar');
    if (!response.ok) throw new Error('Não foi possível carregar o menu lateral');

    root.innerHTML = await response.text();
    setActiveRoute(root);
  } catch (error) {
    console.error('Erro ao carregar sidebar:', error);
  }
}

function setActiveRoute(root) {
  let path = window.location.pathname || '/';

  if (path.length > 1 && path.endsWith('/')) {
    path = path.slice(0, -1);
  }

  const links = root.querySelectorAll('.sidebar-link');

  links.forEach(link => {
    let route = link.getAttribute('data-route') || '/';

    if (route.length > 1 && route.endsWith('/')) {
      route = route.slice(0, -1);
    }

    if (route === path) {
      link.classList.add('active');
    }
  });
}

document.addEventListener('DOMContentLoaded', loadSidebar);